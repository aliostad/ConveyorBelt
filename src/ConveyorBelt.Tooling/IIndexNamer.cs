using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Configuration;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling
{
    public interface IIndexNamer
    {
        string BuildName(DateTimeOffset time, string typeName);
        
        Tuple<string, string> GetIndexNameAndTypeName(DynamicTableEntity entity, DiagnosticsSourceSummary source);

        string GetIndexName(DateTimeOffset time, DiagnosticsSource source);
    }
}
