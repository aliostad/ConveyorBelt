using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Actors;
using BeeHive.Azure;
using BeeHive.Configuration;
using BeeHive.DataStructures;
using BeeHive.ServiceLocator.Windsor;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using ConveyorBelt.Tooling;
using ConveyorBelt.Tooling.Actors;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Parsing;
using ConveyorBelt.Tooling.Scheduling;
using ConveyorBelt.Tooling.Telemetry;

namespace ConveyorBelt.ConsoleWorker
{
    class AppSettingsConfigProvider : IConfigurationValueProvider
    {
        public string GetValue(string name)
        {
            return ConfigurationManager.AppSettings[name];
        }
    }

    class Program
    {

        private static readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private static readonly ManualResetEvent _runCompleteEvent = new ManualResetEvent(false);
        private static IConfigurationValueProvider _configurationValueProvider;
        private static Orchestrator _orchestrator;
        private static MasterScheduler _scheduler;

        protected static void Setup()
        {
            var container = new WindsorContainer();
            var serviceLocator = new WindsorServiceLocator(container);

            _configurationValueProvider = new AppSettingsConfigProvider();
            var storageConnectionString = _configurationValueProvider.GetValue(ConfigurationKeys.StorageConnectionString);
            var servicebusConnectionString = _configurationValueProvider.GetValue(ConfigurationKeys.ServiceBusConnectionString);
            var headersText = _configurationValueProvider.GetValue(ConfigurationKeys.TabSeparatedCustomEsHttpHeaders);
            var headers = new List<KeyValuePair<string, string>>();
            if (headersText != null)
            {
                foreach (var header in headersText.Split('\t'))
                {
                    var strings = header.Split(new[] { ": " }, StringSplitOptions.RemoveEmptyEntries);
                    if (strings.Length == 2)
                        headers.Add(new KeyValuePair<string, string>(strings[0], strings[1]));
                }
            }

            int bulkBatchSize = 100;
            var bulkBatchSizeString = _configurationValueProvider.GetValue(ConfigurationKeys.BulkBatchSize);
            int.TryParse(bulkBatchSizeString, out bulkBatchSize);

            container.Register(
                 Component.For<IElasticsearchClient>()
                    .ImplementedBy<ElasticsearchClient>()
                    .LifestyleSingleton(),
                 Component.For<Orchestrator>()
                    .ImplementedBy<Orchestrator>()
                    .LifestyleSingleton(),
                 Component.For<MasterScheduler>()
                    .ImplementedBy<MasterScheduler>()
                    .LifestyleSingleton(),
                 Component.For<IConfigurationValueProvider>()
                    .Instance(_configurationValueProvider),
                Component.For<ISourceConfiguration>()
                    .ImplementedBy<TableStorageConfigurationSource>(),
                Component.For<IServiceLocator>()
                    .Instance(serviceLocator),
                Component.For<IActorConfiguration>()
                    .Instance(
                    ActorDescriptors.FromAssemblyContaining<ShardRangeActor>()
                    .ToConfiguration().UpdateParallelism(_configurationValueProvider)),
                Component.For<IFactoryActor>()
                    .ImplementedBy<FactoryActor>()
                    .LifestyleTransient(),
                Component.For<IHttpClient>()
                    .ImplementedBy<DefaultHttpClient>()
                    .LifestyleSingleton()
                    .DependsOn(Dependency.OnValue("defaultHeaders", headers)),
                Component.For<ShardKeyActor>()
                    .ImplementedBy<ShardKeyActor>()
                    .LifestyleTransient(),
                Component.For<ShardRangeActor>()
                    .ImplementedBy<ShardRangeActor>()
                    .LifestyleTransient(),
                Component.For<BlobFileActor>()
                    .ImplementedBy<BlobFileActor>()
                    .LifestyleTransient(),
                Component.For<BlobFileConventionActor>()
                    .ImplementedBy<BlobFileConventionActor>()
                    .LifestyleTransient(),
                Component.For<IisBlobScheduler>()
                    .ImplementedBy<IisBlobScheduler>()
                    .LifestyleTransient(),
                Component.For<IisBlobConventionScheduler>()
                    .ImplementedBy<IisBlobConventionScheduler>()
                    .LifestyleTransient(),
                Component.For<IIndexNamer>()
                    .ImplementedBy<IndexNamer>()
                    .LifestyleSingleton(),
                Component.For<RangeShardKeyScheduler>()
                    .ImplementedBy<RangeShardKeyScheduler>()
                    .LifestyleTransient(),
                Component.For<MinuteTableShardScheduler>()
                    .ImplementedBy<MinuteTableShardScheduler>()
                    .LifestyleTransient(),                
                Component.For<Modulo10MinuteTableShardScheduler>()
                    .ImplementedBy<Modulo10MinuteTableShardScheduler>()
                    .LifestyleTransient(),
                Component.For<SimpleBlobScheduler>()
                    .ImplementedBy<SimpleBlobScheduler>()
                    .LifestyleTransient(),
                Component.For<ReverseTimestampMinuteTableShardScheduler>()
                    .ImplementedBy<ReverseTimestampMinuteTableShardScheduler>()
                    .LifestyleTransient(),
                Component.For<D18MinuteTableShardScheduler>()
                    .ImplementedBy<D18MinuteTableShardScheduler>()
                    .LifestyleTransient(),
                Component.For<EventHubScheduler>()
                    .ImplementedBy<EventHubScheduler>()
                    .LifestyleTransient(),
                Component.For<InsightMetricsParser>()
                    .ImplementedBy<InsightMetricsParser>()
                    .LifestyleTransient(),
                Component.For<IisLogParser>()
                    .ImplementedBy<IisLogParser>()
                    .LifestyleTransient(),
                Component.For<AkamaiLogParser>()
                    .ImplementedBy<AkamaiLogParser>()
                    .LifestyleTransient(),
                Component.For<NestBatchPusher>()
                    .ImplementedBy<NestBatchPusher>()
                    .LifestyleTransient()
                    .DependsOn(Dependency.OnValue("esUrl", _configurationValueProvider.GetValue(ConfigurationKeys.ElasticSearchUrl)))
                    .DependsOn(Dependency.OnValue("batchSize", bulkBatchSize)),
                Component.For<ILockStore>()
                    .Instance(new AzureLockStore(new BlobSource()
                    {
                        ConnectionString = storageConnectionString,
                        ContainerName = "locks",
                        Path = "conveyor_belt/locks/master_Keys/"
                    })),
                Component.For<IKeyValueStore>()
                    .Instance(new AzureKeyValueStore(storageConnectionString, "locks")),
                Component.For<IEventQueueOperator>()
                    .Instance(new ServiceBusOperator(servicebusConnectionString)),
                Component.For<ITelemetryProvider>()
                    .ImplementedBy<TelemetryProvider>()
                    .LifestyleSingleton()
                );

            _orchestrator = container.Resolve<Orchestrator>();
            _scheduler = container.Resolve<MasterScheduler>();

            ServicePointHelper.ApplyStandardSettings(_configurationValueProvider.GetValue(ConfigurationKeys.ElasticSearchUrl));
        }

        public static void Run()
        {
            
            Trace.TraceInformation("ConveyorBelt.Worker is running");
            try
            {
                RunAsync(_cancellationTokenSource.Token).Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            finally
            {
                _runCompleteEvent.Set();
            }
        }

        private static void Start()
        {
            Trace.TraceInformation("ConveyorBelt.Worker has been started");
            Task.Run(() => _orchestrator.SetupAsync()).Wait();
            _orchestrator.Start();
        }

        static void Stop()
        {
            _orchestrator.Stop();  
            _cancellationTokenSource.Cancel();
        }

        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0] == "trace")
            {
                TheTrace.Tracer = (level, format, formatParams) =>
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    var msg = formatParams == null || formatParams.Length == 0
                        ? format
                        : string.Format(format, formatParams);

                    Console.WriteLine(msg);
                };
            }

            Setup();
            Start();
            Console.WriteLine("Running. Please press <ENTER> to stop...");
            Run();
            Console.Read();
            Stop();
        }

        private static async Task RunAsync(CancellationToken cancellationToken)
        {

            // schedule every 30 seconds or so
            while (!cancellationToken.IsCancellationRequested)
            {
                Console.WriteLine("Now doing it...");
                var then = DateTimeOffset.UtcNow;
                await _scheduler.ScheduleSourcesAsync();
                var seconds = DateTimeOffset.UtcNow.Subtract(then).TotalSeconds;
                if (seconds < 30)
                {
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    await Task.Delay(TimeSpan.FromSeconds(30 - seconds), cancellationToken);
                    Console.WriteLine("Waiting ...");
                }
            }
        }
    }
}
