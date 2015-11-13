using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using Microsoft.WindowsAzure.Storage.Table;
using Dapper;

namespace ConveyorBelt.Tooling.Querying
{
    public class SqlShardKeyQuery : IShardKeyQuery
    {
        public async Task<IEnumerable<DynamicTableEntity>> QueryAsync(ShardKeyArrived shardKeyArrived)
        {
            var shard = shardKeyArrived.ShardKey; // this is ticks of that minute
            var thatMinute = new DateTime(long.Parse(shard));
            var tableName = (string)shardKeyArrived.Source.GetDynamicProperty(ConveyorBeltConstants.TableName);
            var shardFieldName = (string)shardKeyArrived.Source.GetDynamicProperty(ConveyorBeltConstants.ShardFieldName);
            var idFieldName =
                (string) shardKeyArrived.Source.GetDynamicProperty(ConveyorBeltConstants.IdFieldName);
            var timestampFieldName =
                (string) shardKeyArrived.Source.GetDynamicProperty(ConveyorBeltConstants.TimestampFieldName);

            var entities = new List<DynamicTableEntity>();
            var connection = new SqlConnection(shardKeyArrived.Source.ConnectionString);
            connection.Open();
            var fieldIndices = new Dictionary<string,int>();
            using (connection)
            {
                var sql = string.Format("SELECT * FROM {0} WHERE {1} = '{2}'", tableName, shardFieldName, thatMinute);
                TheTrace.TraceInformation("SQL is {0}", sql);

                var reader = await connection.ExecuteReaderAsync(sql);
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    fieldIndices.Add(reader.GetName(i), i);
                }

                int count = 0;
                while (reader.Read())
                {
                    var data = new Dictionary<string, EntityProperty>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        data.Add(reader.GetName(i), EntityProperty.CreateEntityPropertyFromObject(reader[i]));
                    }

                    foreach (var kv in data)
                    {
                        TheTrace.TraceInformation("Key=>'{0}'    value=>'{1}'", kv.Key, kv.Value);
                    }
                    TheTrace.TraceInformation("timestampFieldName => '{0}'", data[timestampFieldName].ToString());

                    var entity = new DynamicTableEntity(data[shardFieldName].DateTime.Value.ToString("yyyyMMddHHmmss"), data[idFieldName].GuidValue.ToString());
                    entity.Timestamp = new DateTimeOffset(data[timestampFieldName].DateTime.Value);

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        entity.Properties.Add(reader.GetName(i), EntityProperty.CreateEntityPropertyFromObject(data[reader.GetName(i)]));
                    }

                    entities.Add(entity);
                    count++;
                }

                TheTrace.TraceInformation("Added this many row: " + count);

            }

            return entities;
        }
    }
}
