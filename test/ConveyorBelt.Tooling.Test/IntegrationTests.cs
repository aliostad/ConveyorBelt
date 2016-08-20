using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Scheduling;
using Moq;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class IntegrationTests
    {
        private string _connectionString = null;
        private string _elasticsearchHost = null;

        public IntegrationTests()
        {
            _connectionString = Environment.GetEnvironmentVariable("MasterSchedulerReads_cn");
            if(string.IsNullOrWhiteSpace(_connectionString))
                throw new Exception("PLEASE SETUP ENVVAR!!! MasterSchedulerReads_cn");
            _elasticsearchHost = Environment.GetEnvironmentVariable("MasterSchedulerReads_es");
            if (string.IsNullOrWhiteSpace(_connectionString))
                throw new Exception("PLEASE SETUP ENVVAR!!! MasterSchedulerReads_es");
        }

        [Fact]
        public void MasterSchedulerReads()
        {
            var mockQ = new Mock<IEventQueueOperator>();
            var mockCfg = new Mock<IConfigurationValueProvider>();
            mockCfg.Setup(x => x.GetValue(It.Is<string>(c => c == ConfigurationKeys.StorageConnectionString))).Returns(_connectionString);
            mockCfg.Setup(x => x.GetValue(It.Is<string>(c => c == ConfigurationKeys.TableName))).Returns("DagnosticsSourceSimpleBlob");
            mockCfg.Setup(x => x.GetValue(It.Is<string>(c => c == ConfigurationKeys.ElasticSearchUrl))).Returns(_elasticsearchHost);
            var sources = new TableStorageConfigurationSource(mockCfg.Object);
            var mockES = new Mock<IElasticsearchClient>();
            var mockSL = new Mock<IServiceLocator>();
            var scheduler = new MasterScheduler(mockQ.Object, mockCfg.Object, sources, mockES.Object, mockSL.Object);


            scheduler.ScheduleSourcesAsync().Wait();
        }
    }
}
