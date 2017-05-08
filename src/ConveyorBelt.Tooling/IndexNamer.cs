using System;
using BeeHive.Configuration;
using ConveyorBelt.Tooling.Configuration;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling
{
    public class IndexNamer: IIndexNamer
    {
        private readonly string _indexPrefix;
        private readonly bool _oneIndexPerType;

        public IndexNamer(IConfigurationValueProvider configurationValueProvider)
        {
            _indexPrefix = configurationValueProvider.GetValue(ConfigurationKeys.EsIndexPrefix);
            var value = configurationValueProvider.GetValue(ConfigurationKeys.EsOneIndexPerType);
            _oneIndexPerType = Convert.ToBoolean(value);
        }

        public string BuildName(DateTimeOffset time, string typeName)
        {
            var dateString = time.ToString("yyyyMMdd");
            var realIndexName = _oneIndexPerType
                ? $"{_indexPrefix}{typeName}-{dateString}" // [prefix][TypeName]-YYYYMMDD
                : _indexPrefix + dateString;
            return realIndexName;
        }
        
        public Tuple<string, string> GetIndexNameAndTypeName(DynamicTableEntity entity, DiagnosticsSourceSummary source)
        {
            string indexName = null;
            string indexTypeName = null;

            if (_oneIndexPerType)
            {
                var indexPerTypeIndexNameAndIndexType =
                    GetIndexPerTypeIndexNameAndIndexType(entity.Timestamp, source);
                indexName = indexPerTypeIndexNameAndIndexType.Item1;
                indexTypeName = indexPerTypeIndexNameAndIndexType.Item2;
            }
            else
            {
                indexName = source.IndexName ?? BuildName(entity.Timestamp, source.TypeName);
                indexTypeName = source.TypeName;
            }
            return new Tuple<string, string>(indexName, indexTypeName);
        }

        public string GetIndexName(DateTimeOffset time, DiagnosticsSource source)
        {
            if (_oneIndexPerType)
            {
                var mappingName = source.GetMappingName();
                var dateString = time.ToString("yyyyMMdd");
                var realIndexName = $"{_indexPrefix}{mappingName.ToLowerInvariant()}-{dateString}";
                return realIndexName;
            }

            return BuildName(time, source.ToTypeKey());
        }

        private Tuple<string, string> GetIndexPerTypeIndexNameAndIndexType(DateTimeOffset time,
            DiagnosticsSourceSummary source)
        {
            var mappingName = source.DynamicProperties["MappingName"].ToString();
            var dateString = time.ToString("yyyyMMdd");
            var realIndexName = $"{_indexPrefix}{mappingName.ToLowerInvariant()}-{dateString}"; // [prefix][TypeName]-YYYYMMDD
            var indexTypeName = mappingName;

            return new Tuple<string, string>(realIndexName, indexTypeName);
        }
    }
}
