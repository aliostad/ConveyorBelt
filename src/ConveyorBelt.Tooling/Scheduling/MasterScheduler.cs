using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using BeeHive.DataStructures;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Telemetry;
using PerfIt;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class MasterScheduler
    {
        private readonly IEventQueueOperator _eventQueueOperator;
        private readonly IConfigurationValueProvider _configurationValueProvider;
        private readonly IElasticsearchClient _elasticsearchClient;
        private readonly IServiceLocator _locator;
        private readonly ISourceConfiguration _sourceConfiguration;
        private readonly IHttpClient _nonAuthenticatingClient = new DefaultHttpClient(); // TODO: make this 
        private readonly ILockStore _lockStore;
        private readonly ITelemetryProvider _telemetryProvider;
        private readonly SimpleInstrumentor _scheduleDurationInstrumentor;
        private string _createIndexJsonCommand = null;
        private readonly object _padLock = new object();
        private readonly IIndexNamer _indexNamer;


        public MasterScheduler(IEventQueueOperator eventQueueOperator, 
            IConfigurationValueProvider configurationValueProvider,
            ISourceConfiguration sourceConfiguration,
            IElasticsearchClient elasticsearchClient,
            IServiceLocator locator,
            ILockStore lockStore,
            ITelemetryProvider telemetryProvider,
            IIndexNamer indexNamer)
        {
            _indexNamer = indexNamer;
            _lockStore = lockStore;
            _telemetryProvider = telemetryProvider;
            _sourceConfiguration = sourceConfiguration;
            _locator = locator;
            _elasticsearchClient = elasticsearchClient;
            _configurationValueProvider = configurationValueProvider;
            _eventQueueOperator = eventQueueOperator;
            _scheduleDurationInstrumentor  = telemetryProvider.GetInstrumentor<MasterScheduler>();
         }

        public async Task ScheduleSourcesAsync()
        {
            var seconds =
                Convert.ToInt32(_configurationValueProvider.GetValue(ConfigurationKeys.ClusterLockDurationSeconds));
            var sources = _sourceConfiguration.GetSources();
            foreach (var source in sources)
            {
                try
                {
                    var lockToken = new LockToken(source.ToTypeKey());

                    if (!(await _lockStore.TryLockAsync(lockToken, tries: 0, timeoutMilliseconds: seconds * 1000))) // if tries < 1 it puts to 1 in beehive
                    {
                        TheTrace.TraceInformation("I could NOT be master for {0}", source.ToTypeKey());
                        continue;
                    }

                    var resultSource = await TryScheduleSourceAsync(source);
                    if (resultSource != null)
                    {
                        _sourceConfiguration.UpdateSource(resultSource);
                        TheTrace.TraceInformation("MasterScheduler - Updated {0}", resultSource.ToTypeKey());
                    }

                    await _lockStore.ReleaseLockAsync(lockToken);
                }
                catch (Exception e)
                {
                    TheTrace.TraceError(e.ToString());
                }
            }
        }

        private async Task<DiagnosticsSource> TryScheduleSourceAsync(DiagnosticsSource source)
        {
            try
            {
                source = _sourceConfiguration.RefreshSource(source);

                TheTrace.TraceInformation("MasterScheduler - Scheduling {0}", source.ToTypeKey());

                if (!source.IsActive.HasValue || !source.IsActive.Value)
                {
                    TheTrace.TraceInformation("MasterScheduler - NOT active: {0}", source.ToTypeKey());                        
                    return null;                        
                }

                await SetupMappingsAsync(source);

                if (!source.LastScheduled.HasValue)
                    source.LastScheduled = DateTimeOffset.UtcNow.AddDays(-1);

                // if has been recently scheduled
                if (source.LastScheduled.Value.AddMinutes(source.SchedulingFrequencyMinutes.Value) >
                    DateTimeOffset.UtcNow)
                {
                    TheTrace.TraceInformation("MasterScheduler - Nothing to do with {0}. LastScheduled in Future {1}", 
                        source.ToTypeKey(), source.LastScheduled.Value);
                    return null;                        
                }

                _telemetryProvider.WriteTelemetry(
                    "MasterScheduler duration since last scheduled",
                    (long)(DateTime.UtcNow - source.LastScheduled).Value.TotalMilliseconds, 
                    source.ToTypeKey());

                var schedulerType = Assembly.GetExecutingAssembly().GetType(source.SchedulerType) ??
                                    Type.GetType(source.SchedulerType);
                if (schedulerType == null)
                {
                    source.ErrorMessage = "Could not find SchedulerType: " + source.SchedulerType;
                }
                else
                {
                    await _scheduleDurationInstrumentor.InstrumentAsync(async () =>
                    {
                        var scheduler = (ISourceScheduler) _locator.GetService(schedulerType);
                        var result = await scheduler.TryScheduleAsync(source);
                        source.LastScheduled = DateTimeOffset.UtcNow;
                        TheTrace.TraceInformation(
                            "MasterScheduler - Got result for TryScheduleAsync in {0}. Success => {1}",
                            source.ToTypeKey(), result.Item1);

                        if (result.Item2)
                        {
                            await _eventQueueOperator.PushBatchAsync(result.Item1);
                        }

                        source.ErrorMessage = string.Empty;
                        TheTrace.TraceInformation("MasterScheduler - Finished Scheduling {0}", source.ToTypeKey());
                    }, source.ToTypeKey());
                }
                   
                return source;
            }
            catch (Exception e)
            {
                TheTrace.TraceError(e.ToString());
                source.ErrorMessage = e.ToString();
                return source;
            }
        }

        private async Task<string> GetIndexSettings()
        {
            const string defaultSettingsJsonFileName = "__index_settings";
            var settingsJson = _configurationValueProvider.GetValue(ConfigurationKeys.EsIndexCreationJsonFileName);
            if (string.IsNullOrEmpty(settingsJson))
            {
                settingsJson = defaultSettingsJsonFileName;
            }

            var jsonPath = string.Format("{0}{1}.json",
                        _configurationValueProvider.GetValue(ConfigurationKeys.MappingsPath), settingsJson);

            var response = await _nonAuthenticatingClient.GetAsync(jsonPath);

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                TheTrace.TraceWarning("Could not find the index settings file: {0}", jsonPath);
                return string.Empty;
            }

            if (response.Content == null)
                throw new ApplicationException(response.ToString());

            var content = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException(content);

            TheTrace.TraceInformation("This is the index settings JSON: {0}", content);

            return content;

        } 

        private async Task SetupMappingsAsync(DiagnosticsSource source)
        {
            foreach (var indexName in GetIndexNames(source))
            {
                var esUrl = _configurationValueProvider.GetValue(ConfigurationKeys.ElasticSearchUrl);
                await _elasticsearchClient.CreateIndexIfNotExistsAsync(esUrl, indexName, await GetIndexSettings());

                var createMappings = _configurationValueProvider.GetValue(ConfigurationKeys.EsCreateMappings);

                if (Convert.ToBoolean(createMappings) && (!await _elasticsearchClient.MappingExistsAsync(esUrl, indexName, source.ToTypeKey())))
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

            TheTrace.TraceInformation("MasterScheduler - Finished Mapping setup: {0}", source.ToTypeKey());
        }

        public IEnumerable<string> GetIndexNames(DiagnosticsSource source, int daysToGoBack = 7)
        {
            // static index name
            if (!string.IsNullOrEmpty(source.IndexName))
                return new[] { source.IndexName };

            if (String.IsNullOrEmpty(source.LastOffsetPoint))
                source.LastOffsetPoint = DateTimeOffset.UtcNow.AddDays(-daysToGoBack).ToString("O");

            var dateTimeOffset = FileOffset.Parse(source.LastOffsetPoint);

            var days = (int)(DateTimeOffset.UtcNow.AddDays(1) - dateTimeOffset.TimeOffset).TotalDays + 1; // to cover today as well - Aboo was here
            if (days <= 0)
                return Enumerable.Empty<string>();

            return Enumerable.Range(0, days).Select(x => DateTimeOffset.UtcNow.AddDays(1).AddDays(-x))
                .Select(z => _indexNamer.BuildName(dateTimeOffset.TimeOffset, source.ToTypeKey()));
        }

    }
}
