using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using BeeHive.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using Moq;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class ElasticBatchPusherTests
    {
        [Fact]
        public void TriesMultipleTimes()
        {
            var client = new Mock<IHttpClient>(MockBehavior.Loose);
            var indexNamer = new IndexNamer(new AzureConfigurationValueProvider());
            var pusher = new ElasticsearchBatchPusher(client.Object, new AzureConfigurationValueProvider(),  "http://google.com", indexNamer);
            var summary = new DiagnosticsSourceSummary()
            {
                ConnectionString = String.Empty,
                PartitionKey = "pk",
                RowKey = "rk"
            };
            
            pusher.PushAsync(new DynamicTableEntity("pk", "rk"), summary).Wait();
            pusher.PushAsync(new DynamicTableEntity("pk", "rk"), summary).Wait();
            pusher.PushAsync(new DynamicTableEntity("pk", "rk"), summary).Wait();
            pusher.PushAsync(new DynamicTableEntity("pk", "rk"), summary).Wait();
            client.Setup(x => x.PostAsync(It.IsAny<string>(), It.IsAny<HttpContent>())).ReturnsAsync(
                new HttpResponseMessage(HttpStatusCode.Accepted)
                {
                    Content = new StringContent(File.ReadAllText(@"data\es_response_sumsuxes.json"))
                });

            Assert.Throws<AggregateException>(() => pusher.FlushAsync().Wait());

        }

        [Fact]
        public void TriesMultipleTimesAndSucceed()
        {
            var client = new Mock<IHttpClient>(MockBehavior.Loose);
            var indexNamer = new IndexNamer(new AzureConfigurationValueProvider());
            var pusher = new ElasticsearchBatchPusher(client.Object, new AzureConfigurationValueProvider(),  "http://google.com", indexNamer);
            var summary = new DiagnosticsSourceSummary()
            {
                ConnectionString = String.Empty,
                PartitionKey = "pk",
                RowKey = "rk"
            };

            pusher.PushAsync(new DynamicTableEntity("pk", "rk"), summary).Wait();
            pusher.PushAsync(new DynamicTableEntity("pk", "rk"), summary).Wait();
            pusher.PushAsync(new DynamicTableEntity("pk", "rk"), summary).Wait();
            pusher.PushAsync(new DynamicTableEntity("pk", "rk"), summary).Wait();
            client.Setup(x => x.PostAsync(It.IsAny<string>(), It.IsAny<HttpContent>())).ReturnsAsync(
                new HttpResponseMessage(HttpStatusCode.Accepted)
                {
                    Content = new StringContent(File.ReadAllText(@"data\es_response_allsuxes.json"))
                });

            pusher.FlushAsync().Wait();

        }
    }
}
