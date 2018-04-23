using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using ConveyorBelt.Tooling.Entities;
using Newtonsoft.Json;

namespace ConveyorBelt.Tooling.Parsing
{
    public class InsightMetricsParser : IParser
    {
        public IEnumerable<IDictionary<string, string>> Parse(Func<Stream> streamFactory,
            Uri id,
            DiagnosticsSourceSummary source,
            ParseCursor cursor = null)
        {
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

            var ms = new MemoryStream();
            var body = streamFactory();
            body.CopyTo(ms);
            ms.Position = 0;
            var text = new StreamReader(ms).ReadToEnd();
            var col = JsonConvert.DeserializeObject<InsightMetricCollection>(text);
   
            foreach (var r in col.Records)
            {
                var subscriptionGuidFirstPart = r.ResourceId.Split('/')[2].Split('-')[0];
                var pk = string.Format($"{subscriptionGuidFirstPart}_{string.Join("_", r.ResourceId.Split('/').Reverse().Take(3))}_{r.MetricName}");
                var rk = r.Time.ToString("yyyyMMddHHmmss");

                yield return new Dictionary<string, string>()
                {
                    {"@timestamp", r.Time.ToString()},
                    {"PartitionKey", pk},
                    {"RowKey", rk},
                    {"metricName", r.MetricName},
                    {"resourceId", r.ResourceId},
                    {"timeGrain", r.TimeGrain},
                    {"count", r.Count.ToString(CultureInfo.InvariantCulture)},
                    {"total", r.Total.ToString(CultureInfo.InvariantCulture)},
                    {"minimum", r.Minimum.ToString(CultureInfo.InvariantCulture)},
                    {"maximum", r.Maximum.ToString(CultureInfo.InvariantCulture)},
                    {"average", r.Average.ToString(CultureInfo.InvariantCulture)}
                };
            }
        }
    }
}
