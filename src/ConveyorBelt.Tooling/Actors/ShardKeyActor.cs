using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Querying;
using ConveyorBelt.Tooling.Telemetry;
using Microsoft.WindowsAzure.Storage.Table;
using PerfIt;

namespace ConveyorBelt.Tooling.Actors
{

    [ActorDescription("ShardKeyArrived-Process", 5)]
    public class ShardKeyActor : IProcessorActor
    {
        private readonly ITelemetryProvider _telemetryProvider;
        private readonly SimpleInstrumentor _durationInstrumentor;
        private readonly int _shardKeyDelayWarning = 120;
        private readonly NestBatchPusher _pusher;

        public ShardKeyActor(ITelemetryProvider telemetryProvider,
                             IConfigurationValueProvider configurationValueProvider,
							 NestBatchPusher pusher)
        {
            _pusher = pusher;
            _telemetryProvider = telemetryProvider;
            _durationInstrumentor = telemetryProvider.GetInstrumentor<ShardKeyActor>();
            if (!int.TryParse(
                configurationValueProvider.GetValue(ConfigurationKeys.ShadKeyArrivalDelayWarningInSeconds),
                out _shardKeyDelayWarning))
            {
                _shardKeyDelayWarning = 120;
            }
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

                var shardKeyTime = shardKeyArrived.GetDateTimeOffset().ToString("yyyyMMddHHmm");

                await _pusher.PushAll(PreprocessEntities(entities, shardKeyArrived, shardKeyTime), shardKeyArrived.Source).ConfigureAwait(false);
            }).ConfigureAwait(false);

            return Enumerable.Empty<Event>();
        }

        private IEnumerable<DynamicTableEntity> PreprocessEntities(IEnumerable<DynamicTableEntity> entities, ShardKeyArrived shardKeyArrived, string shardKeyTime)
        {
            var minDateTime = DateTimeOffset.MaxValue;
            var n = 0;

            foreach (var entity in entities)
            {
                var eventDateTimeOffset = entity.GetEventDateTimeOffset();
                var delayInSeconds = entity.Timestamp.Subtract(eventDateTimeOffset).TotalSeconds;
                if (delayInSeconds >= _shardKeyDelayWarning)
                {
                    TheTrace.TraceWarning(
                        "SHARD_KEY_ACTOR_DELAY_DETECTED => Delay of {0} seconds for {1} in shardKey {2} and time {3}",
                        delayInSeconds, shardKeyArrived.Source.TypeName, shardKeyArrived.ShardKey, shardKeyTime);
                }

                entity.Timestamp = eventDateTimeOffset;
                yield return entity;

                minDateTime = minDateTime > entity.Timestamp ? entity.Timestamp : minDateTime;
                n++;
            }

            TheTrace.TraceInformation("Gathered {0} records for {1} and ShardKey {2} => {1}_{2} {1}_{3}", n,
                shardKeyArrived.Source.TypeName, shardKeyArrived.ShardKey, shardKeyTime);

            if (n > 0)
            {
                _telemetryProvider.WriteTelemetry(
                    "ShardKeyArrivedActor log delay duration",
                    (long)(DateTimeOffset.UtcNow - minDateTime).TotalMilliseconds,
                    shardKeyArrived.Source.TypeName);
            }
        }
    }
}
