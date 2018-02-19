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

            if (source.IsActive.GetValueOrDefault(true))
            {
                var consume = EventHubConsumer.Consumers.AddOrUpdate(key,
                    new Lazy<EventHubConsumer>(() =>
                    {
                        TheTrace.TraceInformation("Just added this eventHub consumer {0}", key);
                        return new EventHubConsumer(_pusher, source.ToSummary());
                    }),
                    (kk, vv) => vv);

                // to make sure it gets accessed and created if new otherwise system is 'lazy'
                TheTrace.TraceInformation("This is the EventHub thing I was talking about: {0}", consume.Value.Source.TypeName);
            }
            else
            {
                Lazy<EventHubConsumer> consumer = null;

                if (EventHubConsumer.Consumers.TryRemove(key, out consumer))
                {
                    consumer.Value.Dispose();
                    TheTrace.TraceInformation("Just removed this eventHub consumer {0}", key);
                }
            }

            return Task.FromResult(new Tuple<IEnumerable<Event>, bool>(new Event[0], false));
        }
    }
}
