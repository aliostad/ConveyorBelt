using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("ShardRangeArrived-Process", 10)]
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

        public Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            throw new NotImplementedException();
        }
    }
}
