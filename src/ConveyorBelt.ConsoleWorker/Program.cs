using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
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
using ConveyorBelt.Tooling.Parsing;
using ConveyorBelt.Tooling.Scheduling;

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
                    .ToConfiguration()),
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
                Component.For<RangeShardKeyScheduler>()
                    .ImplementedBy<RangeShardKeyScheduler>()
                    .LifestyleTransient(),
                Component.For<MinuteTableShardScheduler>()
                    .ImplementedBy<MinuteTableShardScheduler>()
                    .LifestyleTransient(),
                Component.For<ReverseTimestampMinuteTableShardScheduler>()
                    .ImplementedBy<ReverseTimestampMinuteTableShardScheduler>()
                    .LifestyleTransient(),
                Component.For<IisLogParser>()
                    .ImplementedBy<IisLogParser>()
                    .LifestyleTransient(),
                Component.For<IElasticsearchBatchPusher>()
                    .ImplementedBy<ElasticsearchBatchPusher>()
                    .LifestyleTransient()
                    .DependsOn(Dependency.OnValue("esUrl", _configurationValueProvider.GetValue(ConfigurationKeys.ElasticSearchUrl))),
                Component.For<ILockStore>()
                    .Instance(new AzureLockStore(new BlobSource()
                    {
                        ConnectionString = storageConnectionString,
                        ContainerName = "locks",
                        Path = "conveyor_belt/locks/master_Keys/"
                    })),
                Component.For<IEventQueueOperator>()
                    .Instance(new ServiceBusOperator(servicebusConnectionString))

                );

            _orchestrator = container.Resolve<Orchestrator>();
            _scheduler = container.Resolve<MasterScheduler>();
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
