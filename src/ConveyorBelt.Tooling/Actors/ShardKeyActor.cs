using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Querying;
using ConveyorBelt.Tooling.Telemetry;
using PerfIt;

namespace ConveyorBelt.Tooling.Actors
{

    [ActorDescription("ShardKeyArrived-Process", 5)]
    public class ShardKeyActor : IProcessorActor
    {
        private readonly IElasticsearchBatchPusher _pusher;
        private readonly ITelemetryProvider _telemetryProvider;
        private readonly SimpleInstrumentor _durationInstrumentor;

        public ShardKeyActor(IElasticsearchBatchPusher pusher,
                             ITelemetryProvider telemetryProvider)
        {
            _pusher = pusher;
            _telemetryProvider = telemetryProvider;
            _durationInstrumentor = telemetryProvider.GetInstrumentor<ShardKeyActor>();
        }

        public void Dispose()
        {
        }

        public async Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            var shardKeyArrived = evnt.GetBody<ShardKeyArrived>();
            _telemetryProvider.WriteTelemetry(
               "ShardKey receive message delay duration",
               (long)(DateTime.UtcNow - evnt.Timestamp).TotalMilliseconds, 
               shardKeyArrived.Source.TypeName);

            await _durationInstrumentor.InstrumentAsync(async () =>
            {
                TheTrace.TraceInformation("Got {0} from {1}", shardKeyArrived.ShardKey,
                shardKeyArrived.Source.TypeName);

                var shardKeyQuerier = (string)shardKeyArrived.Source.GetDynamicProperty(ConveyorBeltConstants.ShardKeyQuery);
                var query = FactoryHelper.Create<IShardKeyQuery>(shardKeyQuerier, typeof(TableStorageShardKeyQuery));
                var entities = await query.QueryAsync(shardKeyArrived);

                var minDateTime = DateTimeOffset.MaxValue;
                var hasAnything = false;
                foreach (var entity in entities)
                {
                    await _pusher.PushAsync(entity, shardKeyArrived.Source);
                    hasAnything = true;
                    minDateTime = minDateTime > entity.Timestamp ? entity.Timestamp : minDateTime;
                }

                if (hasAnything)
                {
                    await _pusher.FlushAsync();

                    _telemetryProvider.WriteTelemetry(
                        "ShardKeyArrivedActor log delay duration",
                        (long)(DateTimeOffset.UtcNow - minDateTime).TotalMilliseconds, 
                        shardKeyArrived.Source.TypeName);
                }
            });

            return Enumerable.Empty<Event>();
        }
    }
}
