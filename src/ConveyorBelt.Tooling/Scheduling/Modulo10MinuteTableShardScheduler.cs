using System;
using System.Collections.Generic;
using BeeHive.Configuration;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class Modulo10MinuteTableShardScheduler : MinuteTableShardScheduler
    {
        public Modulo10MinuteTableShardScheduler(IConfigurationValueProvider configurationValueProvider) : base(configurationValueProvider)
        {
        }

        protected override IEnumerable<string> GetShardKeys(DateTimeOffset offset)
        {
            return new[]
            {
                $"0000000000000000000___{offset.Ticks:D19}",
                $"0000000000000000001___{offset.Ticks:D19}",
                $"0000000000000000002___{offset.Ticks:D19}",
                $"0000000000000000003___{offset.Ticks:D19}",
                $"0000000000000000004___{offset.Ticks:D19}",
                $"0000000000000000005___{offset.Ticks:D19}",
                $"0000000000000000006___{offset.Ticks:D19}",
                $"0000000000000000007___{offset.Ticks:D19}",
                $"0000000000000000008___{offset.Ticks:D19}",
                $"0000000000000000009___{offset.Ticks:D19}"
            };
        }
    }
}