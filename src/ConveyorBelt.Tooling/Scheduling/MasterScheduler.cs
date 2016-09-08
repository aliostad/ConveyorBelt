using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using ConveyorBelt.Tooling.Configuration;


namespace ConveyorBelt.Tooling.Scheduling
{
    public class MasterScheduler
    {
        private IEventQueueOperator _eventQueueOperator;
        private IConfigurationValueProvider _configurationValueProvider;
        private IElasticsearchClient _elasticsearchClient;
        private IServiceLocator _locator;
        private ISourceConfiguration _sourceConfiguration;
        private IHttpClient _nonAuthenticatingClient = new DefaultHttpClient(); // TODO: make this 

        public MasterScheduler(IEventQueueOperator eventQueueOperator, 
            IConfigurationValueProvider configurationValueProvider,
            ISourceConfiguration sourceConfiguration,
            IElasticsearchClient elasticsearchClient,
            IServiceLocator locator)
        {
            _sourceConfiguration = sourceConfiguration;
            _locator = locator;
            _elasticsearchClient = elasticsearchClient;
            _configurationValueProvider = configurationValueProvider;
            _eventQueueOperator = eventQueueOperator;
         }

        public async Task ScheduleSourcesAsync()
        {
            var sources = _sourceConfiguration.GetSources();
            foreach (var source in sources)
            {
                Func<Task> unlock = null;
                try
                {

                    TheTrace.TraceInformation("MasterScheduler - Scheduling {0}", source.ToTypeKey());

                    if (!source.IsActive.HasValue || !source.IsActive.Value)
                    {
                        TheTrace.TraceInformation("MasterScheduler - NOT active: {0}", source.ToTypeKey());                        
                        continue;                        
                    }

                    await SetupMappingsAsync(source);
                    TheTrace.TraceInformation("MasterScheduler - Finished Mapping setup: {0}", source.ToTypeKey());


                    if (!source.LastScheduled.HasValue)
                        source.LastScheduled = DateTimeOffset.UtcNow.AddYears(-1);

                    // if has been recently scheduled
                    if (source.LastScheduled.Value.AddMinutes(source.SchedulingFrequencyMinutes.Value) >
                        DateTimeOffset.UtcNow)
                    {
                        TheTrace.TraceInformation("MasterScheduler - Nothing to do with {0}. LastScheduled in Future {1}", 
                            source.ToTypeKey(), source.LastScheduled.Value);
                        continue;                        
                    }

                    var schedulerType = Assembly.GetExecutingAssembly().GetType(source.SchedulerType) ??
                                        Type.GetType(source.SchedulerType);
                    if (schedulerType == null)
                    {
                        source.ErrorMessage = "Could not find SchedulerType: " + source.SchedulerType;
                    }
                    else
                    {
                        var scheduler = (ISourceScheduler)_locator.GetService(schedulerType);
                        var result = await scheduler.TryScheduleAsync(source);
                        TheTrace.TraceInformation(
                            "MasterScheduler - Got result for TryScheduleAsync in {0}. Success => {1}",
                            source.ToTypeKey(), result.Item1);

                        unlock = result.Item3;
                        if (result.Item2)
                        {
                            await _eventQueueOperator.PushBatchAsync(result.Item1);
                        }

                        source.ErrorMessage = string.Empty;
                        TheTrace.TraceInformation("MasterScheduler - Finished Scheduling {0}", source.ToTypeKey());
                    }
                   
                    source.LastScheduled = DateTimeOffset.UtcNow;
                }
                catch (Exception e)
                {
                    TheTrace.TraceError(e.ToString());
                    source.ErrorMessage = e.ToString();
                }

                _sourceConfiguration.UpdateSource(source);
                
                if(unlock!=null)
                    await unlock();
                
                TheTrace.TraceInformation("MasterScheduler - Updated {0}", source.ToTypeKey());
            }
        }


        private async Task SetupMappingsAsync(DiagnosticsSource source)
        {
            
            foreach (var indexName in source.GetIndexNames())
            {
                var esUrl = _configurationValueProvider.GetValue(ConfigurationKeys.ElasticSearchUrl);
                await _elasticsearchClient.CreateIndexIfNotExistsAsync(esUrl, indexName);
                if (!await _elasticsearchClient.MappingExistsAsync(esUrl, indexName, source.ToTypeKey()))
                {
                    var jsonPath = string.Format("{0}{1}.json",
                        _configurationValueProvider.GetValue(ConfigurationKeys.MappingsPath),
                        source.GetMappingName());

                    var response = await _nonAuthenticatingClient.GetAsync(jsonPath);

                    if (response.Content == null)
                        throw new ApplicationException(response.ToString());

                    var content = await response.Content.ReadAsStringAsync();

                    if(!response.IsSuccessStatusCode)
                        throw new InvalidOperationException(content);

                    var mapping = content.Replace("___type_name___", source.ToTypeKey());
                    await _elasticsearchClient.UpdateMappingAsync(esUrl, indexName, source.ToTypeKey(), mapping);
                }
            }
        }
    }
}
