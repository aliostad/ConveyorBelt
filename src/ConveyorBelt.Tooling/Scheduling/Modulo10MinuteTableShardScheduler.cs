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
                string.Format("0000000000000000000___{0:D19}", offset.Ticks),
                string.Format("0000000000000000001___{0:D19}", offset.Ticks),
                string.Format("0000000000000000002___{0:D19}", offset.Ticks),
                string.Format("0000000000000000003___{0:D19}", offset.Ticks),
                string.Format("0000000000000000004___{0:D19}", offset.Ticks),
                string.Format("0000000000000000005___{0:D19}", offset.Ticks),
                string.Format("0000000000000000006___{0:D19}", offset.Ticks),
                string.Format("0000000000000000007___{0:D19}", offset.Ticks),
                string.Format("0000000000000000008___{0:D19}", offset.Ticks),
                string.Format("0000000000000000009___{0:D19}", offset.Ticks)
            };
        }
    }
}