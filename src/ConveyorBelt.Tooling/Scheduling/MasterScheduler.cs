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

        public MasterScheduler(IEventQueueOperator eventQueueOperator, IConfigurationValueProvider configurationValueProvider)
        {
            _configurationValueProvider = configurationValueProvider;
            _eventQueueOperator = eventQueueOperator;
            var account = CloudStorageAccount.Parse(_configurationValueProvider.GetValue(ConfigurationKeys.StorageConnectionString));
            var client = account.CreateCloudTableClient();
            _table = client.GetTableReference(_configurationValueProvider.GetValue(ConfigurationKeys.TableName));

        }

        public async Task ScheduleSourcesAsync()
        {
            var sources = _table.ExecuteQuery(new TableQuery<DynamicTableEntity>()).Select(x => new DiagnosticsSource(x));
            foreach (var source in sources)
            {
                try
                {
                    await SetupMappingsAsync(source);

                    if (!source.LastScheduled.HasValue)
                        source.LastScheduled = DateTimeOffset.UtcNow.AddYears(-1);

                    if (source.LastScheduled.Value.AddMinutes(source.SchedulingFrequencyMinutes.Value) > DateTimeOffset.UtcNow)
                        continue;

                    var schedulerType = Assembly.GetExecutingAssembly().GetType(source.SchedulerType) ??
                                        Type.GetType(source.SchedulerType);
                    if (schedulerType == null)
                    {
                        source.ErrorMessage = "Could not find SchedulerType: " + source.SchedulerType;
                    }
                    var scheduler = (ISourceScheduler)Activator.CreateInstance(schedulerType);
                    var result = await scheduler.TryScheduleAsync(source);
                    if (result.Item2)
                    {
                        await _eventQueueOperator.PushBatchAsync(result.Item1);
                    }

                    source.ErrorMessage = string.Empty;

                }
                catch (Exception e)
                {
                    TheTrace.TraceError(e.ToString());
                    source.ErrorMessage = e.ToString();
                }

                _table.Execute(TableOperation.InsertOrReplace(source.ToEntity()));
                
            }
            
        }


        private async Task SetupMappingsAsync(DiagnosticsSource source)
        {
            
            foreach (var indexName in source.GetIndexNames())
            {
                string indexUrl = string.Format("{0}{1}/",
                    _configurationValueProvider.GetValue(ConfigurationKeys.ElasticSearchUrl),
                    indexName);

                string mappingUrl = string.Format("{2}{0}/{1}/_mapping",
                    indexName,
                    source.ToTypeKey(),
                    _configurationValueProvider.GetValue(ConfigurationKeys.ElasticSearchUrl));

                var response = await _httpClient.GetAsync(mappingUrl);

                string result = null;

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    result = await response.Content.ReadAsStringAsync();
                }

                if (response.StatusCode == HttpStatusCode.NotFound || result == "{}")
                {
                    var jsonPath = string.Format("{0}{1}.json",
                        _configurationValueProvider.GetValue(ConfigurationKeys.MappingsPath),
                        source.GetMappingName());

                    var client = new WebClient();
                    var mapping = client.DownloadString(jsonPath).Replace("___type_name___", source.ToTypeKey());
                    try
                    {
                        client.UploadString(indexUrl, "PUT", "");
                    }
                    catch
                    {
                        // already exists
                    }
                    try
                    {
                        client.UploadString(mappingUrl, "PUT", mapping);
                    }
                    catch (Exception e)
                    {
                        string message = e.ToString();
                        var webException = e as WebException;
                        if (webException != null)
                        {
                            message = new StreamReader(webException.Response.GetResponseStream()).ReadToEnd();
                        }
                        TheTrace.TraceError(message);
                        throw;
                    }

                }

            }

        }

    }
}
