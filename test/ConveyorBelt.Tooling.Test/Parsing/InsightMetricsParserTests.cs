using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using System.IO;
using ConveyorBelt.Tooling.Parsing;
using Newtonsoft.Json;

namespace ConveyorBelt.Tooling.Test.Parsing
{
    public class InsightMetricsParserTests
    {
        private const string FileName = @"data\InsightMetrics.json";

        [Fact]
        public void CanReadMetrics()
        {          
            var stream = File.OpenRead(FileName);
            /*
             * 
 			"count": 4,
			"total": 126,
			"minimum": 0,
			"maximum": 63,
			"average": 31.5,
			"resourceId": "/SUBSCRIPTIONS/9614FC94-9519-46FA-B7EC-DD1B0411DB13/RESOURCEGROUPS/WHASHA/PROVIDERS/MICROSOFT.CACHE/REDIS/FILLAPDWHASHAPRODUCTSEYHOOACHE",
			"time": "2018-01-18T12:55:00.0000000Z",
			"metricName": "connectedclients",
			"timeGrain": "PT1M"
            */
            try
            {
                var parser = new InsightMetricsParser();
                var records = parser.Parse(stream, null).ToList();

                Assert.Equal(96, records.Count);
                var r = records[0];
                var ts = r.Timestamp;

                Assert.Equal("/SUBSCRIPTIONS/9614FC94-9519-46FA-B7EC-DD1B0411DB13/RESOURCEGROUPS/WHASHA/PROVIDERS/MICROSOFT.CACHE/REDIS/FILLAPDWHASHAPRODUCTSEYHOOACHE", r.Properties["resourceId"].StringValue);
                Assert.Equal("connectedclients", r.Properties["metricName"].StringValue);
                Assert.Equal("FILLAPDWHASHAPRODUCTSEYHOOACHE_REDIS_MICROSOFT.CACHE_connectedclients", r.PartitionKey);
                Assert.Equal("20180118125500", r.RowKey);
                Assert.Equal(TimeSpan.Zero, r.Timestamp.Offset);
                Assert.Equal(1, r.Timestamp.Month);
                Assert.Equal(55, r.Timestamp.Minute);
            }
            finally
            {
                stream.Close();
            }
        }

        [Fact]
        public void KeysCreatedQueEsUnico()
        {
            var stream = File.OpenRead(FileName);
            
            try
            {
                var parser = new InsightMetricsParser();
                var records = parser.Parse(stream, null).ToList();

                var dic = records.ToDictionary(x => x.PartitionKey + x.RowKey);
                Assert.Equal(96, dic.Count);
            }
            finally
            {
                stream.Close();
            }

        }
    }
}
