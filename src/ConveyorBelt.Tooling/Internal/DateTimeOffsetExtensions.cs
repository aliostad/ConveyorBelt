using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Events;

namespace ConveyorBelt.Tooling.Internal
{
    internal static class DateTimeOffsetExtensions
    {

        public static int GetFullNumberOfHoursInBetween(this DateTimeOffset from, DateTimeOffset until)
        {
            var maxUntil = new DateTimeOffset(until.Year, until.Month, until.Day, until.Hour, 59, 59, 999, until.Offset);
            return (int) (maxUntil - from).TotalHours;
        }

        public static DateTimeOffset DropSecondAndMilliseconds(this DateTimeOffset offset)
        {
            return new DateTimeOffset(offset.Year,
                offset.Month,
                offset.Day,
                offset.Hour,
                offset.Minute,
                0,
                0,
                offset.Offset);
        }

        public static DateTimeOffset GetDateTimeOffset(this ShardKeyArrived shardKeyArrived)
        {
            return new DateTimeOffset(new DateTime(Convert.ToInt64(shardKeyArrived.ShardKey)), TimeSpan.Zero);
        }
    }
}
