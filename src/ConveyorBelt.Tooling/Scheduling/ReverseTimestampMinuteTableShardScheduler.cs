using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive.DataStructures;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class ReverseTimestampMinuteTableShardScheduler : MinuteTableShardScheduler
    {
        public ReverseTimestampMinuteTableShardScheduler(ILockStore lockStore) : base(lockStore)
        {
        }

        protected override string GetShardKey(DateTimeOffset offset)
        {
            return (DateTimeOffset.MaxValue.Ticks - offset.Ticks).ToString("D19");
        }
    }
}
