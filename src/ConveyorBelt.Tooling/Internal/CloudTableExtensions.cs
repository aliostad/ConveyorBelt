using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Internal
{
    public static class CloudTableExtensions
    {
        public static async Task<IEnumerable<DynamicTableEntity>> ExecuteQueryAsync(this CloudTable table, TableQuery query, CancellationToken ct = default(CancellationToken))
        {
            var items = new List<DynamicTableEntity>();
            TableContinuationToken token = null;

            do
            {
                var seg = await table.ExecuteQuerySegmentedAsync(query, token, ct).ConfigureAwait(false);
                token = seg.ContinuationToken;
                items.AddRange(seg);

            } while (token != null && !ct.IsCancellationRequested);

            return items;
        }
    }
}
