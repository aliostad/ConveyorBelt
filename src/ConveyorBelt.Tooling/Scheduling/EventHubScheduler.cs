using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.EventHub;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class EventHubScheduler : ISourceScheduler
    {
        private readonly IElasticsearchBatchPusher _pusher;

        public EventHubScheduler(IElasticsearchBatchPusher pusher)
        {
            this._pusher = pusher;
        }

        public Task<Tuple<IEnumerable<Event>, bool>> TryScheduleAsync(DiagnosticsSource source)
        {
            var key = source.ToTypeKey();
            EventHubConsumer.Consumers.AddOrUpdate(key,
                (k) => new EventHubConsumer(_pusher, source.ToSummary()),
                (kk, vv) => vv);

            return Task.FromResult(new Tuple<IEnumerable<Event>, bool>(new Event[0], true));
        }
    }
}
