using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Events;
using Xunit;
using Xunit.Extensions;
using ConveyorBelt.Tooling.Internal;

namespace ConveyorBelt.Tooling.Test
{
    public class DateTimeOffsetExtensionsTests
    {
        [Theory]
        [InlineData("2016-02-12 05:02:06", "2016-02-12 06:01:06", 1)]
        [InlineData("2016-02-11 05:02:06", "2016-02-12 06:01:06", 25)]
        [InlineData("2016-02-11 05:59:59", "2016-02-12 06:00:00", 25)]
        [InlineData("2016-02-11 05:00:00", "2016-02-12 05:59:59", 24)]
        public void CalculatesCorrectly(string from, string until, int hours)
        {
            var fromd = DateTimeOffset.Parse(from);
            var untild = DateTimeOffset.Parse(until);
            Assert.Equal(hours, fromd.GetFullNumberOfHoursInBetween(untild));
        }

        [Fact]
        public void DateTimeOffset_GetsReturnedCorrectly()
        {
            
            var shardKeyArrived = new ShardKeyArrived()
            {
                ShardKey = "0635901169200000000"
            };

            Assert.Equal("201602031722", shardKeyArrived.GetDateTimeOffset().ToString("yyyyMMddHHmm"));
        }
    }
}
