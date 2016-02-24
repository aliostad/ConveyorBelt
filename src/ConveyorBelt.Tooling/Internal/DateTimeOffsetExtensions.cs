using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Internal
{
    internal static class DateTimeOffsetExtensions
    {

        public static int GetFullNumberOfHoursInBetween(this DateTimeOffset from, DateTimeOffset until)
        {
            var maxUntil = new DateTimeOffset(until.Year, until.Month, until.Day, until.Hour, 59, 59, 999, until.Offset);
            return (int) (maxUntil - from).TotalHours;
        }
    }
}
