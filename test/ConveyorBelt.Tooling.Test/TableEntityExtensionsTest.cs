using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Internal;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class TableEntityExtensionsTest
    {

        private readonly DateTimeOffset _date = new DateTimeOffset(1969, 8, 18, 9, 0, 0, TimeSpan.Zero); // last morning at woodstock, Jimi Hendrix playing

        [Fact]
        public void GetEventDateTimeOffset_ByDefaultGetsTimestamp()
        {
            var entity = new DynamicTableEntity("foo", "bar")
            {
                Timestamp = _date
            };

            Assert.Equal(_date, entity.GetEventDateTimeOffset());
        }

        [Fact]
        public void GetEventDateTimeOffset_UsesEventDate()
        {
            var entity = new DynamicTableEntity("foo", "bar")
            {
                Timestamp = _date
            };

            var itsYesterday = _date.Subtract(TimeSpan.FromDays(1));
            entity.Properties.Add("EventDate", EntityProperty.GeneratePropertyForDateTimeOffset(itsYesterday));

            Assert.Equal(itsYesterday, entity.GetEventDateTimeOffset());
        }
        [Fact]
        public void GetEventDateTimeOffset_UsesEventTickCount()
        {
            var entity = new DynamicTableEntity("foo", "bar")
            {
                Timestamp = _date
            };

            var itsYesterday = _date.Subtract(TimeSpan.FromDays(1));
            entity.Properties.Add("EventTickCount", EntityProperty.GeneratePropertyForLong(itsYesterday.Ticks));

            Assert.Equal(itsYesterday, entity.GetEventDateTimeOffset());
        }

    }
}
