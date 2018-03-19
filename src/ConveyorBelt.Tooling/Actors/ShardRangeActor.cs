using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("ShardRangeArrived-Process", 3)]
    public class ShardRangeActor : IProcessorActor
    {
        private readonly NestBatchPusher _pusher;

        public ShardRangeActor(NestBatchPusher pusher)
        {
            _pusher = pusher;
        }

        public void Dispose() { }

        public async Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            var shardKeyArrived = evnt.GetBody<ShardRangeArrived>();
            TheTrace.TraceInformation("Got {0}->{1} from {2}", shardKeyArrived.InclusiveStartKey,
                shardKeyArrived.InclusiveEndKey, shardKeyArrived.Source.TypeName);

            //var account = CloudStorageAccount.Parse(shardKeyArrived.Source.ConnectionString);
            CloudStorageAccount account;
            if (!String.IsNullOrWhiteSpace(shardKeyArrived.Source.AccountSasKey))
            {
                // Create new storage credentials using the SAS token.
                var accountSas = new StorageCredentials(shardKeyArrived.Source.AccountSasKey);
                // Use these credentials and the account name to create a Blob service client.
                account = new CloudStorageAccount(accountSas, shardKeyArrived.Source.AccountName, "", useHttps: true);
            }
            else
            {
                account = CloudStorageAccount.Parse(shardKeyArrived.Source.ConnectionString);
            }

            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference(shardKeyArrived.Source.DynamicProperties["TableName"].ToString());

            var entities = table.ExecuteQuery(new TableQuery().Where(
                TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", "ge", shardKeyArrived.InclusiveStartKey), 
                TableOperators.And,
                TableQuery.GenerateFilterCondition("PartitionKey", "le", shardKeyArrived.InclusiveEndKey))));


            await _pusher.PushAll(entities, shardKeyArrived.Source);
            return Enumerable.Empty<Event>();
        }
    }
}
