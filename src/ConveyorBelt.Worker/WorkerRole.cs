using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Actors;
using BeeHive.Azure;
using BeeHive.Configuration;
using BeeHive.DataStructures;
using BeeHive.ServiceLocator.Windsor;
using Castle.Windsor;
using ConveyorBelt.Tooling;
using ConveyorBelt.Tooling.Actors;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Castle.MicroKernel.Registration;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Parsing;
using ConveyorBelt.Tooling.Scheduling;

namespace ConveyorBelt.Worker
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
        private IConfigurationValueProvider _configurationValueProvider;
        private Orchestrator _orchestrator;
        private MasterScheduler _scheduler;

        public WorkerRole()
        {
            var container = new WindsorContainer();
            var serviceLocator = new WindsorServiceLocator(container);

            _configurationValueProvider = new AzureConfigurationValueProvider();
            var storageConnectionString = _configurationValueProvider.GetValue(ConfigurationKeys.StorageConnectionString);
            var clusterLockContainer = _configurationValueProvider.GetValue(ConfigurationKeys.ClusterLockContainer);
            var clusterLockRootPath = _configurationValueProvider.GetValue(ConfigurationKeys.ClusterLockRootPath);
            var headersText = _configurationValueProvider.GetValue(ConfigurationKeys.TabSeparatedCustomEsHttpHeaders);
            var headers = new List<KeyValuePair<string, string>>();
            if (headersText != null)
            {
                foreach (var header in headersText.Split('\t'))
                {
                    var strings = header.Split(new []{ ": "}, StringSplitOptions.RemoveEmptyEntries);
                    if (strings.Length == 2)
                        headers.Add(new KeyValuePair<string, string>( strings[0], strings[1]));
                }
            }

            int bulkBatchSize = 100;
            var bulkBatchSizeString = _configurationValueProvider.GetValue(ConfigurationKeys.BulkBatchSize);
            int.TryParse(bulkBatchSizeString, out bulkBatchSize);

            var servicebusConnectionString = _configurationValueProvider.GetValue(ConfigurationKeys.ServiceBusConnectionString);

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
                Component.For<IServiceLocator>()
                    .Instance(serviceLocator),
                Component.For<IActorConfiguration>()
                    .Instance(
                    ActorDescriptors.FromAssemblyContaining<ShardRangeActor>()
                    .ToConfiguration().UpdateParallelism(_configurationValueProvider)),
                Component.For<ISourceConfiguration>()
                    .ImplementedBy<TableStorageConfigurationSource>(),
                Component.For<IFactoryActor>()
                    .ImplementedBy<FactoryActor>()
                    .LifestyleTransient(),
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
                Component.For<SimpleBlobScheduler>()
                    .ImplementedBy<SimpleBlobScheduler>()
                    .LifestyleTransient(),
                Component.For<MinuteTableShardScheduler>()
                    .ImplementedBy<MinuteTableShardScheduler>()
                    .LifestyleTransient(),                
                Component.For<Modulo10MinuteTableShardScheduler>()
                    .ImplementedBy<Modulo10MinuteTableShardScheduler>()
                    .LifestyleTransient(),
                Component.For<ReverseTimestampMinuteTableShardScheduler>()
                    .ImplementedBy<ReverseTimestampMinuteTableShardScheduler>()
                    .LifestyleTransient(),
                Component.For<IisLogParser>()
                    .ImplementedBy<IisLogParser>()
                    .LifestyleTransient(),
                Component.For<AkamaiLogParser>()
                    .ImplementedBy<AkamaiLogParser>()
                    .LifestyleTransient(),
                Component.For<IHttpClient>()
                    .ImplementedBy<DefaultHttpClient>()
                    .LifestyleSingleton()
                    .DependsOn(Dependency.OnValue("defaultHeaders", headers)),
                Component.For<ITempDownloadLocationProvider>()
                    .ImplementedBy<AzureTempDownloadLocationProvider>()
                    .LifestyleSingleton(),                    
                Component.For<IElasticsearchBatchPusher>()
                    .ImplementedBy<ElasticsearchBatchPusher>()
                    .LifestyleTransient()
                    .DependsOn(Dependency.OnValue("esUrl", _configurationValueProvider.GetValue(ConfigurationKeys.ElasticSearchUrl)))
                    .DependsOn(Dependency.OnValue("batchSize", bulkBatchSize)),
                Component.For<ILockStore>()
                    .Instance(new AzureLockStore(new BlobSource()
                    {
                        ConnectionString = storageConnectionString,
                        ContainerName = clusterLockContainer,
                        Path = clusterLockRootPath
                    })),
                Component.For<IEventQueueOperator>()
                    .Instance(new ServiceBusOperator(servicebusConnectionString))
                );

            _orchestrator = container.Resolve<Orchestrator>();
            _scheduler = container.Resolve<MasterScheduler>();
        }

        public override void Run()
        {
            Trace.TraceInformation("ConveyorBelt.Worker is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 1200;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("ConveyorBelt.Worker has been started");
            Task.Run(() => _orchestrator.SetupAsync()).Wait();
            _orchestrator.Start();

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("ConveyorBelt.Worker is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();
            _orchestrator.Stop();

            Trace.TraceInformation("ConveyorBelt.Worker has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {

            // schedule every 30 seconds or so
            while (!cancellationToken.IsCancellationRequested)
            {
                var then = DateTimeOffset.UtcNow;
                await _scheduler.ScheduleSourcesAsync();
                var seconds = DateTimeOffset.UtcNow.Subtract(then).TotalSeconds;
                if (seconds < 30)
                    await Task.Delay(TimeSpan.FromSeconds(30 - seconds), cancellationToken);
            }
        }
    }

    public class AzureTempDownloadLocationProvider : ITempDownloadLocationProvider
    {
        public string GetDownloadFolder()
        {
            return RoleEnvironment.GetLocalResource("DownloadFolder").RootPath;
        }
    }
}

