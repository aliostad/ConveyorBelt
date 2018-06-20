using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Querying
{
    public class TableStorageShardKeyQuery : IShardKeyQuery
    {
        public Task<IEnumerable<DynamicTableEntity>> QueryAsync(ShardKeyArrived shardKeyArrived)
        {
            //var account = CloudStorageAccount.Parse(shardKeyArrived.Source.ConnectionString);
            CloudStorageAccount account = null;
            if (!string.IsNullOrWhiteSpace(shardKeyArrived.Source.AccountSasKey))
            {
                // Create new storage credentials using the SAS token.
                var accountSas = new StorageCredentials(shardKeyArrived.Source.AccountSasKey);
                // Use these credentials and the account name to create a Blob service client.
                try
                {
                    account = new CloudStorageAccount(accountSas, shardKeyArrived.Source.AccountName, endpointSuffix:"", useHttps: true);
                }
                catch (Exception ex)
                {
                    TheTrace.TraceError(ex.ToString());
                }
            }
            else
            {
                account = CloudStorageAccount.Parse(shardKeyArrived.Source.ConnectionString);
            }

            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference(shardKeyArrived.Source.DynamicProperties["TableName"].ToString());

            var query = new TableQuery<DynamicTableEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", "eq", shardKeyArrived.ShardKey));

            return Task.FromResult(table.ExecuteQuery(query));
        }
    }
}
