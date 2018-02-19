using System;
using System.Collections.Generic;
using System.Linq;
using BeeHive;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Configuration
{
    public class DiagnosticsSource
    {
        private readonly DynamicTableEntity _entity;
        internal const string CustomAttributesFieldName = "CustomAttributes";

        public DiagnosticsSource(DynamicTableEntity entity)
        {
            _entity = entity;
        }

        public string SchedulerType
        {
            get { return _entity.Properties.GetStringValue("SchedulerType"); } 
        }

        public string ConnectionString
        {
            get { return _entity.Properties.GetStringValue("ConnectionString"); }
        }

        public string AccountName
        {
            get { return _entity.Properties.GetStringValue("StorageAccountName"); }
        }

        public string AccountSubscriptionId
        {
            get { return _entity.Properties.GetStringValue("AccountSubscriptionId"); }
        }

        public string AccountSasKey
        {
            get { return _entity.Properties.GetStringValue("AccountSasKey"); }
        }
        
        public string CustomAttributes
        {
            get { return _entity.Properties.GetStringValue(CustomAttributesFieldName); }
        }

        public string ErrorMessage
        {
            get { return _entity.Properties.GetStringValue("ErrorMessage"); }
            set { _entity.Properties["ErrorMessage"] = EntityProperty.GeneratePropertyForString(value);}
        }

        public DateTimeOffset? LastScheduled
        {
            get { return _entity.Properties.GetDateTimeValue("LastScheduled"); }
            set { _entity.Properties["LastScheduled"] = EntityProperty.GeneratePropertyForDateTimeOffset(value); }
        }

        public string LastOffsetPoint
        {
            get { return _entity.Properties.GetStringValue("LastOffsetPoint"); }
            set { _entity.Properties["LastOffsetPoint"] = EntityProperty.GeneratePropertyForString(value); }
        }

        public string StopOffsetPoint
        {
            get { return _entity.Properties.GetStringValue("StopOffsetPoint"); }
        }
        
        /// <summary>
        /// Number of minutes after which if no entry exists, it considered done and if there was anything,
        /// would have been copied to TableStorage by now 
        /// </summary>
        public int? GracePeriodMinutes
        {
            get { return _entity.Properties.GetIntValue("GracePeriodMinutes", 5); }
        }

        public int? SchedulingFrequencyMinutes
        {
            get { return _entity.Properties.GetIntValue("SchedulingFrequencyMinutes", 1); }
        }

        public int? MaxItemsInAScheduleRun
        {
            get { return _entity.Properties.GetIntValue("MaxItemsInAScheduleRun"); }
        }

        public bool? IsActive
        {
            get { return _entity.Properties.GetBooleanValue("IsActive", true); }
        }

        public string ToTypeKey()
        {
            return AlternateTypeName ?? (PartitionKey + "_" + RowKey);
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

        public string IndexName
        {
            get { return _entity.Properties.GetStringValue("IndexName"); }
        }

        public string AlternateTypeName
        {
            get
            {
                return _entity.Properties.ContainsKey("AlternateTypeName")
                    ? _entity.Properties["AlternateTypeName"].StringValue
                    : null;
            }
        }

        public DynamicTableEntity ToEntity()
        {
            var entity = new DynamicTableEntity(PartitionKey, RowKey);
            entity.Properties.Add("ErrorMessage", _entity.Properties["ErrorMessage"]);
            entity.Properties.Add("LastScheduled", _entity.Properties["LastScheduled"]);
            entity.Properties.Add("LastOffsetPoint", _entity.Properties.ContainsKey("LastOffsetPoint") ? 
                _entity.Properties["LastOffsetPoint"] : 
                EntityProperty.GeneratePropertyForDateTimeOffset(DateTimeOffset.Now.Subtract(TimeSpan.FromDays(365))));
            entity.ETag = _entity.ETag;
            return entity;
        }

        public T GetProperty<T>(string name)
        {
            if (_entity.Properties.ContainsKey(name))
            {
                try
                {
                    if (typeof (T) == typeof (DateTimeOffset))
                        return (T) (object) new DateTimeOffset((DateTime) _entity.Properties[name].PropertyAsObject);

                    return (T) _entity.Properties[name].PropertyAsObject;
                }
                catch (Exception)
                {
                    TheTrace.TraceError("Failed to convert {0} to {1}", name, typeof(T).Name);   
                    throw;
                }
            }
            else
            {
                return default(T);
            }
        }

        public void SetProperty<T>(string name, T value)
        {
            _entity.Properties[name] = EntityProperty.CreateEntityPropertyFromObject(value);
        }

        public string GetMappingName()
        {
            return GetProperty<string>("MappingName") ??
                   GetProperty<string>("TableName");
        }

        
        public DiagnosticsSourceSummary ToSummary()
        {
            var dss = new DiagnosticsSourceSummary()
            {
                ConnectionString = ConnectionString,
                AccountSasKey = AccountSasKey,
                AccountName = AccountName,
                IndexName = IndexName,
                PartitionKey = PartitionKey,
                RowKey = RowKey,
                TypeName = ToTypeKey(),
                DynamicProperties = new Dictionary<string, object>()
            };

            foreach (var kv in _entity.Properties)
            {
                dss.DynamicProperties.Add(kv.Key, kv.Value.PropertyAsObject);
            }

            return dss;
        }
    }

    static class IDictionaryExtensions
    {
        public static string GetStringValue(this IDictionary<string, EntityProperty> dic, string name, string defaultValue = null)
        {
            return dic.ContainsKey(name)
                ? dic[name].StringValue
                : defaultValue;
        }

        public static int? GetIntValue(this IDictionary<string, EntityProperty> dic, string name, int? defaultValue = null)
        {
            return dic.ContainsKey(name)
                ? dic[name].Int32Value
                : defaultValue;
        }

        public static DateTimeOffset? GetDateTimeValue(this IDictionary<string, EntityProperty> dic, 
            string name, DateTimeOffset? defaultValue = null)
        {
            return dic.ContainsKey(name)
                ? dic[name].DateTimeOffsetValue
                : defaultValue;
        }

        public static bool? GetBooleanValue(this IDictionary<string, EntityProperty> dic,
            string name, bool? defaultValue = null)
        {
            return dic.ContainsKey(name)
                ? dic[name].BooleanValue
                : defaultValue;
        }
    }

}
