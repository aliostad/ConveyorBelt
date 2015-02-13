using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive.Configuration;
using BeeHive.DataStructures;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class ReverseTimestampMinuteTableShardScheduler : MinuteTableShardScheduler
    {
        public ReverseTimestampMinuteTableShardScheduler(ILockStore lockStore, IConfigurationValueProvider configurationValueProvider)
            : base(lockStore, configurationValueProvider)
        {
        }

        protected override string GetShardKey(DateTimeOffset offset)
        {
            return (DateTimeOffset.MaxValue.Ticks - offset.Ticks).ToString("D19");
        }
    }
}
