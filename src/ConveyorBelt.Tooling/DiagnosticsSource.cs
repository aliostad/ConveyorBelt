using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling
{
    public class DiagnosticsSource : TableEntity
    {

        public bool IsRevereseTimestamp { get; set; }

        public string TableName { get; set; }

        public string ConnectionString { get; set; }

        public DateTimeOffset? LastOffset { get; set; }

        /// <summary>
        /// Number of minutes after which if no entry exists, it considered done and if there was anything,
        /// would have been copied to TableStorage by now 
        /// </summary>
        public int GracePeriodMinutes { get; set; }

        public bool IsActive { get; set; }

        public string ToTypeKey()
        {
            return PartitionKey + "_" + RowKey;
        }


    }

}
