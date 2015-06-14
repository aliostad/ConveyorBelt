using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling
{
    public class DiagnosticsSourceSummary
    {

        public string ConnectionString { get; set; }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public string ToTypeKey()
        {
            return PartitionKey + "_" + RowKey;
        }

        public string IndexName { get; set; }

        public IDictionary<string, object> DynamicProperties { get; set; } 
        
    }
}
