using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class MasterScheduler
    {
        protected CloudTable _table;
        private IEventQueueOperator _eventQueueOperator;
        private IConfigurationValueProvider _configurationValueProvider;
        private HttpClient _httpClient = new HttpClient();
        private IElasticsearchClient _elasticsearchClient;
        private IServiceLocator _locator;

        public MasterScheduler(IEventQueueOperator eventQueueOperator, 
            IConfigurationValueProvider configurationValueProvider,
            IElasticsearchClient elasticsearchClient,
            IServiceLocator locator)
        {
            _locator = locator;
            _elasticsearchClient = elasticsearchClient;
            _configurationValueProvider = configurationValueProvider;
            _eventQueueOperator = eventQueueOperator;
            var account = CloudStorageAccount.Parse(_configurationValueProvider.GetValue(ConfigurationKeys.StorageConnectionString));
            var client = account.CreateCloudTableClient();
            _table = client.GetTableReference(_configurationValueProvider.GetValue(ConfigurationKeys.TableName));
            _table.CreateIfNotExists();
        }

        public async Task ScheduleSourcesAsync()
        {
            var sources = _table.ExecuteQuery(new TableQuery<DynamicTableEntity>()).Select(x => new DiagnosticsSource(x));
            foreach (var source in sources)
            {
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
                    var scheduler = (ISourceScheduler) _locator.GetService(schedulerType);
                    var result = await scheduler.TryScheduleAsync(source);
                    TheTrace.TraceInformation(
                        "MasterScheduler - Got result for TryScheduleAsync in {0}. Success => {1}",
                        source.ToTypeKey(), result.Item1);

                    if (result.Item2)
                    {
                        await _eventQueueOperator.PushBatchAsync(result.Item1);
                    }

                    source.ErrorMessage = string.Empty;
                    TheTrace.TraceInformation("MasterScheduler - Finished Scheduling {0}", source.ToTypeKey());

                }
                catch (Exception e)
                {
                    TheTrace.TraceError(e.ToString());
                    source.ErrorMessage = e.ToString();
                }

                _table.Execute(TableOperation.InsertOrReplace(source.ToEntity()));
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
                    var client = new WebClient();
                    var mapping = client.DownloadString(jsonPath).Replace("___type_name___", source.ToTypeKey());
                    await _elasticsearchClient.UpdateMappingAsync(esUrl, indexName, source.ToTypeKey(), mapping);
                }
            }
        }
    }
}
