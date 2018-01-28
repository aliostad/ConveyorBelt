using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Entities
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

    public class InsightMetric
    {
        public string MetricName { get; set; }

        public string ResourceId { get; set; }

        public string TimeGrain { get; set; }

        public Double Average { get; set; }

        public double Minimum { get; set; }

        public double Maximum { get; set; }

        public double Total { get; set; }

        public long Count { get; set; }

        public DateTimeOffset Time { get; set; }
    }

    public class InsightMetricCollection
    {
        public List<InsightMetric> Records { get; set; }
    }
}
