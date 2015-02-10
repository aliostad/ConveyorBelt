using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Azure;
using ConveyorBelt.Tooling.Events;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Actors
{

    [ActorDescription("ShardKeyArrived-Process", 1)]
    public class ShardKeyActor : IProcessorActor
    {
        private IElasticsearchBatchPusher _pusher;

        public ShardKeyActor(IElasticsearchBatchPusher pusher)
        {
            _pusher = pusher;
        }

        public void Dispose()
        {
            
        }

        public async Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            var shardKeyArrived = evnt.GetBody<ShardKeyArrived>();
            TheTrace.TraceInformation("Got {0} from {1}", shardKeyArrived.ShardKey, shardKeyArrived.Source.ToTypeKey());

            var account = CloudStorageAccount.Parse(shardKeyArrived.Source.ConnectionString);
            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference(shardKeyArrived.Source.DynamicProperties["TableName"].ToString());

            var entities = table.ExecuteQuery(new TableQuery().Where(
                TableQuery.GenerateFilterCondition("PartitionKey", "eq", shardKeyArrived.ShardKey)));

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
