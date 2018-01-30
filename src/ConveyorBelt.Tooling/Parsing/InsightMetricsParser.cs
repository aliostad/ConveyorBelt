using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Entities;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ConveyorBelt.Tooling.Parsing
{
    public class InsightMetricsParser : IParser
    {
        public IEnumerable<DynamicTableEntity> Parse(Stream body,
            Uri id,
            long position = 0, 
            long endPosition = 0)
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
            body.CopyTo(ms);
            ms.Position = 0;
            var text = new StreamReader(ms).ReadToEnd();
            var col = JsonConvert.DeserializeObject<InsightMetricCollection>(text);
   
            foreach (var r in col.Records)
            {
                var subscriptionGuidFirstPart = r.ResourceId.Split('/')[2].Split('-')[0];
                var pk = string.Format($"{subscriptionGuidFirstPart}_{string.Join("_", r.ResourceId.Split('/').Reverse().Take(3))}_{r.MetricName}");
                var rk = r.Time.ToString("yyyyMMddHHmmss");
                var entity = new DynamicTableEntity(pk, rk);
                entity.Timestamp = r.Time;
                entity.Properties.Add("metricName", EntityProperty.GeneratePropertyForString(r.MetricName));
                entity.Properties.Add("resourceId", EntityProperty.GeneratePropertyForString(r.ResourceId));
                entity.Properties.Add("timeGrain", EntityProperty.GeneratePropertyForString(r.TimeGrain));
                entity.Properties.Add("count", EntityProperty.GeneratePropertyForLong(r.Count));
                entity.Properties.Add("total", EntityProperty.GeneratePropertyForDouble(r.Total));
                entity.Properties.Add("minimum", EntityProperty.GeneratePropertyForDouble(r.Minimum));
                entity.Properties.Add("maximum", EntityProperty.GeneratePropertyForDouble(r.Maximum));
                entity.Properties.Add("average", EntityProperty.GeneratePropertyForDouble(r.Average));

                yield return entity;
            }
        }
    }
}
