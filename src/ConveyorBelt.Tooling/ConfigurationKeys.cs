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
    }
}
