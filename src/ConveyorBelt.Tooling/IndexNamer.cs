using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BeeHive.Configuration;

namespace ConveyorBelt.Tooling
{
    public class IndexNamer: IIndexNamer
    {
        private readonly string _indexPrefix = string.Empty;
        private readonly bool _oneIndexPerType = false;

        public IndexNamer(IConfigurationValueProvider configurationValueProvider)
        {
            _indexPrefix = configurationValueProvider.GetValue(ConfigurationKeys.EsIndexPrefix);
            var value = configurationValueProvider.GetValue(ConfigurationKeys.EsOneIndexPerType);
            _oneIndexPerType = Convert.ToBoolean(value);
        }

        public string BuildName(DateTimeOffset? time, string typeName)
        {
            return BuildName(time?.ToString("s"), typeName);
        }

        public string BuildName(string timeIso, string typeName)
        {
            typeName = typeName.ToLowerInvariant();
                return _oneIndexPerType ? $"{_indexPrefix}{typeName}" : _indexPrefix;

            var dateString = new string(new [] {
                timeIso[0], timeIso[1], timeIso[2], timeIso[3],
                timeIso[5], timeIso[6],
                timeIso[8], timeIso[9]
            });

            return _oneIndexPerType
                ? $"{_indexPrefix}{typeName}-{dateString}"
                : _indexPrefix + dateString;
        }
    }
}
