using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ConveyorBelt.Tooling
{
    public class ElasticsearchBatchPusher : IElasticsearchBatchPusher
    {
        private string _esUrl;
        private StringBuilder _stringBuilder = new StringBuilder();
        private int _batchSize;
        private int _numberOfRecords = 0;
        private HttpClient _httpClient = new HttpClient();

        public ElasticsearchBatchPusher(string esUrl, int batchSize = 100)
        {
            _batchSize = batchSize;
            _esUrl = esUrl;
        }

        private async Task PushbatchAsync()
        {

            if (_stringBuilder.Length == 0)
                return;

            try
            {
                var responseMessage = await  _httpClient.PostAsync(_esUrl + "_bulk",
                    new StringContent(_stringBuilder.ToString(),
                        Encoding.UTF8, "application/json"));
                _stringBuilder.Clear();
                _numberOfRecords = 0;
                responseMessage.EnsureSuccessStatusCode();

                TheTrace.TraceInformation("ConveyorBelt_Pusher: Pushing to {0}", _esUrl);
            }
            catch (Exception e)
            {
                TheTrace.TraceError(e.ToString());
                throw;
            }
        }

        public async Task PushAsync(DynamicTableEntity entity, DiagnosticsSource source)
        {
            var op = new
            {
                index = new
                {
                    _index = source.IndexName ?? entity.Timestamp.ToString("yyyyMMdd"),
                    _type = source.ToTypeKey(),
                    _id = entity.PartitionKey + entity.RowKey
                }
            };

            var doc = new JObject();
            doc.Add("@timestamp", entity.Timestamp);
            doc.Add("PartitionKey", entity.PartitionKey);
            doc.Add("RowKey", entity.RowKey);
            foreach (var property in entity.Properties)
            {
                doc[property.Key] = JToken.FromObject(property.Value.PropertyAsObject);
            }
            _stringBuilder.Append(JsonConvert.SerializeObject(op).Replace("\r\n", " "));
            _stringBuilder.Append('\n');
            _stringBuilder.Append(doc.ToString().Replace("\r\n", " "));
            _stringBuilder.Append('\n');

            if ((_numberOfRecords++) >= _batchSize)
            {
                await PushbatchAsync();
                TheTrace.TraceInformation("ConveyorBelt_Pusher: Pushed {0} records to ElasticSearch for {1}-{2}",
                    _numberOfRecords,
                    source.PartitionKey,
                    source.RowKey);
            }
        }

        public Task FlushAsync()
        {
            return PushbatchAsync();
        }
    }

}
