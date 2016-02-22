using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Azure;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Querying;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Actors
{

    [ActorDescription("ShardKeyArrived-Process", 5)]
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
            TheTrace.TraceInformation("Got {0} from {1}", shardKeyArrived.ShardKey, 
                shardKeyArrived.Source.TypeName);

            var shardKeyQuerier = (string) shardKeyArrived.Source.GetDynamicProperty(ConveyorBeltConstants.ShardKeyQuery);
            var query = FactoryHelper.Create<IShardKeyQuery>(shardKeyQuerier, typeof (TableStorageShardKeyQuery));
            var entities = await query.QueryAsync(shardKeyArrived);

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
