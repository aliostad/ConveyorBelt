using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
            if (time == null)
                return _oneIndexPerType ? $"{_indexPrefix}{typeName}" : _indexPrefix;

            var dateString = time.Value.ToString("yyyyMMdd");

            var realIndexName = _oneIndexPerType
                ? $"{_indexPrefix}{typeName}-{dateString}" // [prefix][TypeName]-YYYYMMDD
                : _indexPrefix + dateString;

            return realIndexName;
        }
    }
}
