using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.DataStructures;
using ConveyorBelt.Tooling.Events;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class MinuteTableShardScheduler : BaseScheduler
    {
        public MinuteTableShardScheduler(ILockStore lockStore) : base(lockStore)
        {
        }

        protected async override Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            if (source.LastOffsetPoint == null)
                source.LastOffsetPoint = DateTimeOffset.UtcNow.AddDays(-7).ToString();

            var offset = DateTimeOffset.Parse(source.LastOffsetPoint);
            var events = new List<Event>();
            var totalMinutes = DateTimeOffset.UtcNow.Subtract(offset.AddMinutes(source.GracePeriodMinutes.Value)).TotalMinutes;

            var ofsted = new DateTimeOffset();
            for (int i = 0; i < totalMinutes; i++)
            {
                ofsted = offset.AddMinutes(i + 1);
                var shardKey = GetShardKey(ofsted);
                events.Add(new Event(new ShardKeyArrived(){ Source = source.ToSummary(), ShardKey = shardKey}));
                if(source.MaxItemsInAScheduleRun.HasValue && i >= source.MaxItemsInAScheduleRun)
                    break;
            }
            source.LastOffsetPoint = offset.ToString();
            return events;
        }

        protected virtual string GetShardKey(DateTimeOffset offset)
        {
            return offset.Ticks.ToString("D19");
        }
    }
}
