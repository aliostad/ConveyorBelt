using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive.DataStructures;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class D18MinuteTableShardScheduler : MinuteTableShardScheduler
    {
        public D18MinuteTableShardScheduler(ILockStore lockStore) : base(lockStore)
        {
        }

        protected override string GetShardKey(DateTimeOffset offset)
        {
            return offset.Ticks.ToString("D19");
        }

        private DateTimeOffset CleanOffset(DateTimeOffset offset)
        {
            offset = offset.Subtract(TimeSpan.FromMilliseconds(offset.Millisecond));
            return offset.Subtract(TimeSpan.FromSeconds(offset.Second));
        }
    }
}
