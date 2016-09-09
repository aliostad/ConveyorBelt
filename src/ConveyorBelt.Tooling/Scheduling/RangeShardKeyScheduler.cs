using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using BeeHive.DataStructures;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Events;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class RangeShardKeyScheduler : BaseScheduler
    {
        public RangeShardKeyScheduler(IConfigurationValueProvider configurationValueProvider)
            : base(configurationValueProvider)
        {
        }

        protected override Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            var account = CloudStorageAccount.Parse(source.ConnectionString);
            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference(source.GetProperty<string>("TableName"));

            var entities = table.ExecuteQuery(new TableQuery().Where(
               TableQuery.GenerateFilterCondition("PartitionKey", "gt", source.LastOffsetPoint)));

            foreach (var entity in entities)
            {
                
            }
 
            throw new NotImplementedException();
            
        }
    }
}
