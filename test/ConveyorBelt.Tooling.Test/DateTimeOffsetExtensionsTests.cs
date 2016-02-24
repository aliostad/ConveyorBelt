using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        public void CalculatesCorrectly(string from, string until, int hours)
        {
            var fromd = DateTimeOffset.Parse(from);
            var untild = DateTimeOffset.Parse(until);
            Assert.Equal(hours, fromd.GetFullNumberOfHoursInBetween(untild));
        }
    }
}
