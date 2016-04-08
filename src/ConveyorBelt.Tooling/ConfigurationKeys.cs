using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling
{
    public class ConfigurationKeys
    {
        public const string FrequencyInSeconds = "ConveyorBelt.FrequencyInSeconds";
        public const string TableName = "ConveyorBelt.TableName";
        public const string ElasticSearchUrl = "ConveyorBelt.ElasticSearchUrl";
        public const string StorageConnectionString = "ConveyorBelt.Storage.ConnectionString";
        public const string ServiceBusConnectionString = "ConveyorBelt.ServiceBus.ConnectionString";
        public const string MappingsPath = "ConveyorBelt.MappingsPath";
        public const string ClusterLockContainer = "ConveyorBelt.Storage.ClusterLockContainer";
        public const string ClusterLockRootPath = "ConveyorBelt.Storage.ClusterLockRoot";
        public const string ClusterLockDurationSeconds = "ConveyorBelt.Storage.ClusterLockDurationSeconds";
        /// <summary>
        /// These are tab separated with header and value separated by ": " similar to HTTP headers
        /// </summary>
        public const string TabSeparatedCustomEsHttpHeaders = "ConveyorBelt.Storage.TabSeparatedCustomEsHttpHeaders";

        public const string EsBackOffMinSeconds = "ConveyorBelt.ElasticSearch.BackOffMinSeconds";
        public const string EsBackOffMaxSeconds = "ConveyorBelt.ElasticSearch.BackOffMaxSeconds";

    }
}
