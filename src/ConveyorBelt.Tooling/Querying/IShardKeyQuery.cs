using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Events;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Querying
{
    public interface IShardKeyQuery
    {
        Task<IEnumerable<DynamicTableEntity>> QueryAsync(ShardKeyArrived shardKeyArrived);
    }
}
