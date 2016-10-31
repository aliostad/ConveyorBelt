using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Internal
{
    internal static class TableEntityExtensions
    {
        public static DateTimeOffset GetEventDateTimeOffset(this DynamicTableEntity entity)
        {
            const string EventDateFieldName = "EventDate";
            const string EventTickCountFieldName = "EventTickCount";

            // check for event date first
            if (entity.Properties.ContainsKey(EventDateFieldName) && entity.Properties[EventDateFieldName].PropertyType == EdmType.DateTime)
            {
                return entity.Properties[EventDateFieldName].DateTimeOffsetValue.Value;
            }

            // check for event tick count first
            if (entity.Properties.ContainsKey(EventTickCountFieldName) && entity.Properties[EventTickCountFieldName].PropertyType == EdmType.Int64)
            {
                return new DateTimeOffset(new DateTime(entity.Properties[EventTickCountFieldName].Int64Value.Value), TimeSpan.Zero);;
            }

            // OK timestamp is the only thing we can look for then
            return entity.Timestamp;
        }
    }
}
