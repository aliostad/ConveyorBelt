using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling
{
    public class DiagnosticsSource
    {
        private DynamicTableEntity _entity;

        public DiagnosticsSource(DynamicTableEntity entity)
        {
            _entity = entity;
        }

        public string SchedulerType 
        {
            get { return _entity.Properties["SchedulerType"].StringValue; }
            set { _entity.Properties["SchedulerType"].StringValue = value; } 
        }

        public string ConnectionString {
            get { return _entity.Properties["ConnectionString"].StringValue; }
            set { _entity.Properties["ConnectionString"].StringValue = value; }
        }

        public string ErrorMessage
        {
            get { return _entity.Properties["ErrorMessage"].StringValue; }
            set { _entity.Properties["ErrorMessage"].StringValue = value; }
        }

        public DateTimeOffset? LastScheduled
        {
            get { return _entity.Properties["LastScheduled"].DateTimeOffsetValue; }
            set { _entity.Properties["LastScheduled"].DateTimeOffsetValue = value; } 
        }

        public string LastSetpoint
        {
            get { return _entity.Properties["LastSetpoint"].StringValue; }
            set { _entity.Properties["LastSetpoint"].StringValue = value; } 
        }

        /// <summary>
        /// Number of minutes after which if no entry exists, it considered done and if there was anything,
        /// would have been copied to TableStorage by now 
        /// </summary>
        public int? GracePeriodMinutes
        {
            get { return _entity.Properties["GracePeriodMinutes"].Int32Value; }
            set { _entity.Properties["GracePeriodMinutes"].Int32Value = value; } 
        }

        public int? SchedulingFrequencyMinutes
        {
            get { return _entity.Properties["SchedulingFrequencyMinutes"].Int32Value; }
            set { _entity.Properties["SchedulingFrequencyMinutes"].Int32Value = value; }
        }

        public bool? IsActive
        {
            get { return _entity.Properties["IsActive"].BooleanValue; }
            set { _entity.Properties["IsActive"].BooleanValue = value; } 
        }

        public string ToTypeKey()
        {
            return _entity.PartitionKey + "_" + _entity.RowKey;
        }

        public string IndexName
        {
            get { return _entity.Properties["IndexName"].StringValue; }
            set { _entity.Properties["IndexName"].StringValue = value; } 
        }

        public string PartitionKey
        {
            get { return _entity.PartitionKey; }
            set { _entity.PartitionKey = value; } 
        }

        public string RowKey
        {
            get { return _entity.RowKey; }
            set { _entity.RowKey = value; } 
        }

        public DynamicTableEntity ToEntity()
        {
            return _entity;
        }

        public IDictionary<string, EntityProperty> Properties { get { return _entity.Properties; } }

        public IEnumerable<string> GetIndexNames(int daysToGoBack = 7)
        {
            if (!string.IsNullOrEmpty(IndexName))
                return new [] {IndexName};

            if (String.IsNullOrEmpty(LastSetpoint))
                LastSetpoint = DateTimeOffset.UtcNow.AddDays(-daysToGoBack).ToString();
            var dateTimeOffset = DateTimeOffset.Parse(LastSetpoint);

            var days = (int)(DateTimeOffset.UtcNow.AddDays(1) - dateTimeOffset).TotalDays;
            return Enumerable.Range(0, days).Select(x => DateTimeOffset.UtcNow.AddDays(1).AddDays(-x))
                .Select(y => y.ToString("yyyyMMdd"));
        }

        public string GetMappingName()
        {
            return _entity.Properties.ContainsKey("MappingName")
                ? _entity.Properties["MappingName"].StringValue
                : _entity.Properties["TableName"].StringValue;
        }
    }


}
