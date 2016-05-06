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
using ConveyorBelt.Tooling.Internal;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class MinuteTableShardScheduler : BaseScheduler
    {
        public MinuteTableShardScheduler(ILockStore lockStore, IConfigurationValueProvider configurationValueProvider) 
            : base(lockStore, configurationValueProvider)
        {
        }

        protected override Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            if (source.LastOffsetPoint == null)
                source.LastOffsetPoint = DateTimeOffset.UtcNow.AddDays(-7).DropSecondAndMilliseconds().ToString("O");

            var offset = DateTimeOffset.Parse(source.LastOffsetPoint);
            var events = new List<Event>();
            var totalMinutes = DateTimeOffset.UtcNow.Subtract(offset.AddMinutes(source.GracePeriodMinutes.Value)).TotalMinutes;

            var ofsted = DateTimeOffset.UtcNow;
            for (int i = 0; i < totalMinutes; i++)
            {
                ofsted = offset.AddMinutes(i + 1);
                var shardKey = GetShardKey(ofsted);
                events.Add(new Event(new ShardKeyArrived(){ Source = source.ToSummary(), ShardKey = shardKey}));
                if(source.MaxItemsInAScheduleRun.HasValue && i >= source.MaxItemsInAScheduleRun)
                    break;
            }

            source.LastOffsetPoint = ofsted.DropSecondAndMilliseconds().ToString("O");
            return Task.FromResult((IEnumerable<Event>)events);
        }

        protected virtual string GetShardKey(DateTimeOffset offset)
        {
            return offset.Ticks.ToString("D19");
        }
    }
}
