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

        [Fact]
        public void IfTimestampFieldSpecifiedOverridesTimestamp()
        {
            var ahora = DateTimeOffset.Now;

            var jest = new DynamicTableEntity("ali", "ostad", "eTag", new Dictionary<string, EntityProperty>
            {
                {"whah??", EntityProperty.GeneratePropertyForDateTimeOffset(ahora)} 
            });

            jest.Timestamp = ahora.Subtract(TimeSpan.FromDays(42));
            var source = new DiagnosticsSourceSummary();
            source.DynamicProperties[ConveyorBeltConstants.TimestampFieldName] = "whah??";
            Assert.Equal(ahora, jest.GetTimestamp(source));
        }

        [Fact]
        public void NoErroesIfTimestampFieldSpecifiedNotExistsAndGetsBackTimestamp()
        {
            var ahora = DateTimeOffset.Now;

            var jest = new DynamicTableEntity("ali", "ostad", "eTag", new Dictionary<string, EntityProperty>
            {
                {"whah??", EntityProperty.GeneratePropertyForDateTimeOffset(ahora)}
            });

            jest.Timestamp = ahora.Subtract(TimeSpan.FromDays(42));
            var source = new DiagnosticsSourceSummary();
            source.DynamicProperties[ConveyorBeltConstants.TimestampFieldName] = "Non-existing";

            Assert.Equal(ahora.Subtract(TimeSpan.FromDays(42)), jest.GetTimestamp(source));
        }

        [Fact]
        public void ConvertTableEntityToDictionary()
        {
            var ahora = DateTimeOffset.FromFileTime(129000000000000000).UtcDateTime;
            var entity = new DynamicTableEntity("ali", "ostad", "eTag", new Dictionary<string, EntityProperty> {
                {"whah??", EntityProperty.GeneratePropertyForDateTimeOffset(ahora)},
                {"inty", EntityProperty.GeneratePropertyForInt(123)},
                {"doubly", EntityProperty.GeneratePropertyForDouble(123.23)},
                {"booly", EntityProperty.GeneratePropertyForBool(false)},
                {"stringy", EntityProperty.GeneratePropertyForString("magical unicorns")},
                {"ignored1", EntityProperty.GeneratePropertyForString(",")},
                {"ignored2", EntityProperty.GeneratePropertyForString("")}
            }) {
                Timestamp = ahora.Subtract(TimeSpan.FromDays(42))
            };

            var source = new DiagnosticsSourceSummary{ TypeName = "typename" };
            source.DynamicProperties[ConveyorBeltConstants.TimestampFieldName] = "whah??";
            
            var dict = entity.ToDictionary(source);
            
            Assert.Equal("typename", dict["cb_type"]);
            Assert.Equal("ali", dict["PartitionKey"]);
            Assert.Equal("ostad", dict["RowKey"]);
            Assert.Equal(ahora.ToString("s"), dict["whah??"]);
            Assert.Equal("123", dict["inty"]);
            Assert.Equal("123.23", dict["doubly"]);
            Assert.Equal("false", dict["booly"]);
            Assert.Equal("magical unicorns", dict["stringy"]);
            Assert.Equal(ahora.ToString("s"), dict["@timestamp"]);
            Assert.False(dict.ContainsKey("ignored1"));
            Assert.False(dict.ContainsKey("ignored2"));
            Assert.Equal(9, dict.Count);
        }
    }
}
