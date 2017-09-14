using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Configuration
{
    public class TableStorageConfigurationSource : ISourceConfiguration
    {
        private CloudTable _table;
        private object _lock = new object();

        public TableStorageConfigurationSource(IConfigurationValueProvider configurationValueProvider)
        {
            MakeSureTableIsThere(configurationValueProvider);
        }

        public IEnumerable<DiagnosticsSource> GetSources()
        {
            return _table.ExecuteQuery(new TableQuery<DynamicTableEntity>()).Select(x => new DiagnosticsSource(x));
        }

        public void UpdateSource(DiagnosticsSource source)
        {
           // _table.Execute(TableOperation.InsertOrReplace(source.ToEntity()));
            _table.Execute(TableOperation.Merge(source.ToEntity()));
        }

        public DiagnosticsSource RefreshSource(DiagnosticsSource source)
        {
            var s = _table.ExecuteQuery(new TableQuery().Where(TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", "eq", source.PartitionKey),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", "eq", source.RowKey)
                ))).Single();
            return new DiagnosticsSource(s);
        }

        private void MakeSureTableIsThere(IConfigurationValueProvider configurationValueProvider)
        {
            if (_table == null)
            {
                lock (_lock)
                {
                    if (_table == null)
                    {
                        //var account = CloudStorageAccount.Parse(configurationValueProvider.GetValue(ConfigurationKeys.StorageConnectionString));
                        CloudStorageAccount account;
                        if (!String.IsNullOrWhiteSpace(
                            configurationValueProvider.GetValue(ConfigurationKeys.StorageAccountSasKey)))
                        {
                            // Create new storage credentials using the SAS token.
                            var accountSas =
                                new StorageCredentials(
                                    configurationValueProvider.GetValue(ConfigurationKeys.StorageAccountSasKey));
                            // Use these credentials and the account name to create a Blob service client.
                            account = new CloudStorageAccount(accountSas, ConfigurationKeys.StorageAccountName, "",
                                useHttps: true);
                        }
                        else
                        {
                            account =
                                CloudStorageAccount.Parse(
                                    configurationValueProvider.GetValue(ConfigurationKeys.StorageConnectionString));
                        }
                        var client = account.CreateCloudTableClient();
                        _table = client.GetTableReference(configurationValueProvider.GetValue(ConfigurationKeys.TableName));
                        _table.CreateIfNotExists();
                    }
                }
            }        
        }
    }
}
