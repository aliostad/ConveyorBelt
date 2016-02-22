using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("ShardRangeArrived-Process", 3)]
    public class ShardRangeActor : IProcessorActor
    {
        private IElasticsearchBatchPusher _pusher;

        public ShardRangeActor(IElasticsearchBatchPusher pusher)
        {
            _pusher = pusher;
        }

        public void Dispose()
        {
            
        }

        public async Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            var shardKeyArrived = evnt.GetBody<ShardRangeArrived>();
            TheTrace.TraceInformation("Got {0}->{1} from {2}", shardKeyArrived.InclusiveStartKey,
                shardKeyArrived.InclusiveEndKey, shardKeyArrived.Source.TypeName);

            var account = CloudStorageAccount.Parse(shardKeyArrived.Source.ConnectionString);
            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference(shardKeyArrived.Source.DynamicProperties["TableName"].ToString());

            var entities = table.ExecuteQuery(new TableQuery().Where(
                TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", "ge", shardKeyArrived.InclusiveStartKey), 
                TableOperators.And,
                TableQuery.GenerateFilterCondition("PartitionKey", "le", shardKeyArrived.InclusiveEndKey))));

            bool hasAnything = false;
            foreach (var entity in entities)
            {
                await _pusher.PushAsync(entity, shardKeyArrived.Source);
                hasAnything = true;
            }

            if (hasAnything)
            {
                await _pusher.FlushAsync();
            }

            return Enumerable.Empty<Event>();
        }
    }
}
