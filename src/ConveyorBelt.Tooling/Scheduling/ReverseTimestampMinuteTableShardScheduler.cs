using System;
using System.Collections.Generic;
using BeeHive.Configuration;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class ReverseTimestampMinuteTableShardScheduler : MinuteTableShardScheduler
    {
        public ReverseTimestampMinuteTableShardScheduler(IConfigurationValueProvider configurationValueProvider)
            : base(configurationValueProvider)
        {
        }

        protected override IEnumerable<string> GetShardKeys(DateTimeOffset offset)
        {
            return new [] { string.Format("{0:D19}", (DateTimeOffset.MaxValue.Ticks - offset.Ticks)) };
        }
    }
}
