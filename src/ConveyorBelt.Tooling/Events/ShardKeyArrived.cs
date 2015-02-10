using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Events
{
    public class ShardKeyArrived
    {
        public DiagnosticsSourceSummary Source { get; set; }

        public string ShardKey { get; set; }
    }
}
