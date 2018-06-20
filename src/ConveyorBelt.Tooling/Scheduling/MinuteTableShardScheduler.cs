using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class MinuteTableShardScheduler : BaseScheduler
    {
        public MinuteTableShardScheduler(IConfigurationValueProvider configurationValueProvider) 
            : base(configurationValueProvider)
        {
        }

        protected override Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            if (source.LastOffsetPoint == null)
                source.LastOffsetPoint = GetDefaultLastOffset().AddDays(-1).DropSecondAndMilliseconds().ToString("O");

            var lastOffset = DateTimeOffset.Parse(source.LastOffsetPoint);
            var events = new List<Event>();
            var graceMinutes = source.GracePeriodMinutes ?? 3;

            var now = GetDefaultLastOffset().DropSecondAndMilliseconds();
            var newLastOffset = lastOffset;
            int n = 1; // start from a minute after
            while (now >= lastOffset.Add(TimeSpan.FromMinutes(graceMinutes + n)))
            {
                newLastOffset = lastOffset.Add(TimeSpan.FromMinutes(n))
                    .DropSecondAndMilliseconds(); // just to be sure
                var shardKeys = GetShardKeys(newLastOffset);
                events.AddRange(shardKeys.Select(shardKey => new Event(new ShardKeyArrived { Source = source.ToSummary(), ShardKey = shardKey })));
                if (source.MaxItemsInAScheduleRun.HasValue && n >= source.MaxItemsInAScheduleRun)
                    break;
                n++;
                TheTrace.TraceInformation("Scheduling {0} for minute {1} and shardkey {2} => {0}_{1} AND {0}_{2}", source.ToTypeKey(), newLastOffset.ToString("yyyyMMddHHmm"), shardKeys.First());
            }

            source.LastOffsetPoint = newLastOffset.ToString("O");
            return Task.FromResult((IEnumerable<Event>)events);
        }

        protected virtual IEnumerable<string> GetShardKeys(DateTimeOffset offset)
        {
            return new[] { $"{offset.Ticks:D19}" };
        }
    }
}
