using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Scheduling;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Internal;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ConveyorBelt.Tooling
{
    public class ElasticsearchBatchPusher : IElasticsearchBatchPusher
    {
        private string _esUrl;
        private int _batchSize;
        private IHttpClient _httpClient;
        private Batch _batch = new Batch();

        // no reason for thread sync/concurrent since this will be called only by a single thread
        private Dictionary<string, SimpleFilter> _filters = new Dictionary<string, SimpleFilter>();
        private IInterval _interval = new DoublyIncreasingInterval(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(300), 5);

        public ElasticsearchBatchPusher(IHttpClient httpClient, string esUrl, int batchSize = 100)
        {
            _httpClient = httpClient;
            _batchSize = batchSize;
            _esUrl = esUrl;
        }

        private async Task PushbatchAsync()
        {

            if (_batch.Count == 0)
                return;

            try
            {
                int retry = 0;
                List<int> statuses = null;
                do
                {
                    var responseMessage = await  _httpClient.PostAsync(_esUrl + "_bulk",
                    new StringContent(_batch.ToString(),
                        Encoding.UTF8, "application/json"));
                    var content = responseMessage.Content == null ? "" :
                        (await responseMessage.Content.ReadAsStringAsync());

                    if(!responseMessage.IsSuccessStatusCode)
                        throw new ApplicationException(string.Format("Unsuccessful ES bulk: {0} - {1}", responseMessage.StatusCode, content));

                    dynamic j = JObject.Parse(content);
                    if (j == null || j.items == null)
                        throw new ApplicationException(string.Format("Unsuccessful ES bulk - items null: {0}", content));

                    var items = (JArray) j.items;
                    statuses = items.Children<JObject>().Select(x => x.Properties().First().Value["status"].Value<int>()).ToList();

                    if (statuses.Any(y => y >= 200 && y <= 299))
                    {
                       TheTrace.TraceWarning("LOOK!! We had some errors from ES bulk at retry {1}: {0}", content, retry);
                    }

                    if (statuses.Any(x => x == 429))
                    {
                        var timeSpan = _interval.Next();
                        TheTrace.TraceWarning("LOOK!! Got 429 -> backing off for {0} seconds", timeSpan.TotalSeconds);
                        Thread.Sleep(timeSpan);
                    }
                    else
                    {
                        _interval.Reset();
                    }

                    TheTrace.TraceInformation("ConveyorBelt_Pusher: Pushing {1} records to {0} [retry: {2}]", _esUrl, _batch.Count, retry);

                } while (_batch.Prune(statuses) > 0 && retry++ < 3);

                if (_batch.Count > 0)
                {
                    TheTrace.TraceWarning("WARNING!!! Some residual documents could not be inserted even after retries: {0}", _batch.Count);
                    _batch.Clear();
                }
            }
            catch (Exception e)
            {
                TheTrace.TraceError(e.ToString());
                throw;
            }
        }

        public async Task PushAsync(DynamicTableEntity entity, DiagnosticsSourceSummary source)
        {
            if(source.Filter == null)
                source.Filter = string.Empty;

            if (!_filters.ContainsKey(source.Filter))
            {
                _filters.Add(source.Filter, new SimpleFilter(source.Filter));
            }

            if (!_filters[source.Filter].Satisfies(entity))
                return;

            var op = new
            {
                index = new
                {
                    _index = source.IndexName ?? entity.Timestamp.ToString("yyyyMMdd"),
                    _type = source.TypeName,
                    _id = entity.PartitionKey + entity.RowKey
                }
            };

            var doc = new JObject();
            doc.Add("@timestamp", entity.Timestamp);
            doc.Add("PartitionKey", entity.PartitionKey);
            doc.Add("RowKey", entity.RowKey);
            foreach (var property in entity.Properties)
            {
                if (property.Key != DiagnosticsSource.CustomAttributesFieldName)
                    doc[property.Key] = JToken.FromObject(property.Value.PropertyAsObject);
            }
            if (entity.Properties.ContainsKey(DiagnosticsSource.CustomAttributesFieldName))
            {
                foreach (var keyValue in GetNameValues(entity.Properties[DiagnosticsSource.CustomAttributesFieldName].StringValue))
                {
                    doc[keyValue.Key] = keyValue.Value;
                }
            }

            _batch.AddDoc(JsonConvert.SerializeObject(op).Replace("\r\n", " "), doc.ToString().Replace("\r\n", " "));

            if (_batch.Count >= _batchSize)
            {
                await PushbatchAsync();
                TheTrace.TraceInformation("ConveyorBelt_Pusher: Pushed records to ElasticSearch for {0}-{1}",
                    source.PartitionKey,
                    source.RowKey);
            }
        }

        private static IEnumerable<KeyValuePair<string, string>> GetNameValues(string customAttribs)
        {
            if (string.IsNullOrWhiteSpace(customAttribs))
                customAttribs = "";

            return customAttribs.Split(';')
                .Select(s => s.Split('='))
                .Select(ss => new KeyValuePair<string, string>(ss[0], ss.Length == 1 ? string.Empty : ss[1]));
        }

        public Task FlushAsync()
        {
            return PushbatchAsync();
        }

        

        internal class Batch
        {
             private List<Tuple<string, string>> _list = new List<Tuple<string, string>>();

            public void AddDoc(string op, string doc)
            {
                _list.Add(new Tuple<string, string>(op, doc));
            }

            public int Count
            {
                get { return _list.Count; }
            }

            public void Clear()
            {
                _list.Clear();
            }

            public override string ToString()
            {
                var sb = new StringBuilder();
                foreach (var item in _list)
                {
                    sb.Append(item.Item1);
                    sb.Append('\n');
                    sb.Append(item.Item2);
                    sb.Append('\n');
                }

                return sb.ToString();
            }

            public int Prune(IList<int> statuses)
            {
                if(statuses.Count != _list.Count)
                    throw new InvalidOperationException(string.Format(
                        "Statuses should have exactly the same number of items. {0} vs. statuses {1}", _list.Count, statuses.Count));

                for (var i = _list.Count-1; i >= 0; i--) // start from the end
                {
                    var si = statuses[i];
                    if(si >= 200 || si <= 299) // if success
                        _list.RemoveAt(i);
                }

                return _list.Count;
            }
        }
    }
}
