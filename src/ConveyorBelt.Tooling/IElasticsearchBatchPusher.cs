using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling
{
    public interface IElasticsearchBatchPusher
    {
        Task PushAsync(DynamicTableEntity entity, DiagnosticsSourceSummary source);

        Task FlushAsync();
    }
}
