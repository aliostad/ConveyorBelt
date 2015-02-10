using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class DiagnosticsSourceSummaryTests
    {
        [Fact]
        public void CanBeSerialised()
        {
            var summary = GetASummary();
            var sso = JsonConvert.SerializeObject(summary);
        }

        [Fact]
        public void CanBeDeserialised()
        {
            var summary = GetASummary();
            var sso = JsonConvert.SerializeObject(summary);
            var summary2 = JsonConvert.DeserializeObject<DiagnosticsSourceSummary>(sso);

            Assert.Equal(summary.ConnectionString, summary2.ConnectionString);
            Assert.Equal(summary.IndexName, summary2.IndexName);
            Assert.Equal(summary.PartitionKey, summary2.PartitionKey);
            Assert.Equal(summary.RowKey, summary2.RowKey);
            Assert.Equal(summary.DynamicProperties["dpi"], summary2.DynamicProperties["dpi"]);
            Assert.Equal(summary.DynamicProperties["dps"], summary2.DynamicProperties["dps"]);
            Assert.Equal(summary.DynamicProperties["dpd"], summary2.DynamicProperties["dpd"]);
            Assert.Equal(summary.DynamicProperties["dpb"], summary2.DynamicProperties["dpb"]);
        }


        [Fact]
        public void CanConvertFromSourceToSummary()
        {
            var entity = new DynamicTableEntity("pk", "rk");
            entity.Properties["dpi"] = EntityProperty.GeneratePropertyForInt(2);
            entity.Properties["dps"] = EntityProperty.GeneratePropertyForString("man");
            entity.Properties["dpd"] = EntityProperty.GeneratePropertyForDateTimeOffset(DateTime.UtcNow);
            entity.Properties["dpb"] = EntityProperty.GeneratePropertyForBool(true);
            var source = new DiagnosticsSource(entity);

            var summary = source.ToSummary();
            Assert.Equal(summary.ConnectionString, source.ConnectionString);
            Assert.Equal(summary.PartitionKey, source.PartitionKey);
            Assert.Equal(summary.RowKey, source.RowKey);
            Assert.Equal(summary.DynamicProperties["dpd"], source.GetProperty<DateTime>("dpd"));
            Assert.Equal(summary.DynamicProperties["dpi"], source.GetProperty<int>("dpi"));
            Assert.Equal(summary.DynamicProperties["dps"], source.GetProperty<string>("dps"));
            Assert.Equal(summary.DynamicProperties["dpb"], source.GetProperty<bool>("dpb"));
            
        }


        private DiagnosticsSourceSummary GetASummary()
        {
            var summary = new DiagnosticsSourceSummary()
            {
                ConnectionString = "cn",
                IndexName = "in",
                PartitionKey = "pk",
                RowKey = "rk"
            };

            summary.DynamicProperties["dpi"] = (Int64) 2;
            summary.DynamicProperties["dps"] = "man";
            summary.DynamicProperties["dpd"] = DateTime.UtcNow;
            summary.DynamicProperties["dpb"] = true;

            return summary;
        }

    }
}
