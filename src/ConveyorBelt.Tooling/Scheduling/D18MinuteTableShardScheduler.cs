using System;
using System.Collections.Generic;
using BeeHive.Configuration;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class D18MinuteTableShardScheduler : MinuteTableShardScheduler
    {
        public D18MinuteTableShardScheduler(IConfigurationValueProvider configurationValueProvider)
            : base(configurationValueProvider)
        {
        }

        protected override IEnumerable<string> GetShardKeys(DateTimeOffset offset)
        {
            return new [] { string.Format("{0:D18}", offset.Ticks) };
        }
    }
}
