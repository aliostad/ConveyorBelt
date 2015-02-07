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
    public class ElasticsearchBatchPusher
    {
        private string _esUrl;
        private StringBuilder _stringBuilder = new StringBuilder();
        private int _batchSize;
        private int _numberOfRecords = 0;
        private DiagnosticsSource _source;
        private HttpClient _httpClient = new HttpClient();
        private bool _isConnected = false;

        public ElasticsearchBatchPusher(DiagnosticsSource source, string esUrl, int batchSize = 100)
        {
            _source = source;
            _batchSize = batchSize;
            _esUrl = esUrl;
            _isConnected = true;
        }

        public void Push(DynamicTableEntity entity)
        {

            if (!_isConnected)
                throw new InvalidOperationException("Please connect first.");

            var op = new
            {
                index = new
                {
                    _index = entity.Timestamp.ToString("yyyyMMdd"),
                    _type = _source.ToTypeKey(),
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
                Pushbatch();
                TheTrace.TraceInformation("ConveyorBelt_Pusher: Pushed {0} records to ElasticSearch for {1}-{2}",
                    _numberOfRecords,
                    _source.PartitionKey,
                    _source.RowKey);
            }
        }

        private void Pushbatch()
        {

            if (_stringBuilder.Length == 0)
                return;

            try
            {
                var responseMessage = _httpClient.PostAsync(_esUrl + "_bulk",
                    new StringContent(_stringBuilder.ToString(),
                        Encoding.UTF8, "application/json"))
                      .Result;
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

        public void Flush()
        {
            if (!_isConnected)
                throw new InvalidOperationException("Please connect first.");

            Pushbatch();
        }
    }

}
