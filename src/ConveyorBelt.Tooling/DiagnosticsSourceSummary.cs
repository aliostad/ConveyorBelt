using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling
{
    public class DiagnosticsSourceSummary
    {
        public DiagnosticsSourceSummary()
        {
            DynamicProperties = new ConcurrentDictionary<string, object>();
        }

        public string ConnectionString { get; set; }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        /// <summary>
        /// Elasticsearch type name
        /// </summary>
        public string TypeName { get; set; }

        public string IndexName { get; set; }

        /// <summary>
        /// One or more pipe-delimited (OR) expression containing a field name and a value: [field name]=[value]
        /// If set, it will only convey such entries meeting the criteria
        /// </summary>
        public string Filter { get; set; }

        public IDictionary<string, object> DynamicProperties { get; set; } 
        
        public object GetDynamicProperty(string name, object defaultValue = null)
        {
            return (DynamicProperties.ContainsKey(name) ? DynamicProperties[name] : null)
                ?? defaultValue;
        }
    }
}
