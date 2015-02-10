using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Events
{
    public class ShardRangeArrived
    {
        public DiagnosticsSourceSummary Source { get; set; }

        public string InclusiveStartKey { get; set; }

        public string InclusiveEndKey { get; set; }
    }
}
