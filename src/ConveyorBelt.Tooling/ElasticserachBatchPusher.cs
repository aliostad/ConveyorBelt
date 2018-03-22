using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
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
        private bool _setPipeline;
        private IHttpClient _httpClient;
        private Batch _batch = new Batch();

        // no reason for thread sync/concurrent since this will be called only by a single thread
        private ConcurrentDictionary<string, SimpleFilter> _filters = new ConcurrentDictionary<string, SimpleFilter>();
        private IInterval _interval;
        private IIndexNamer _indexNamer;
        private object _lock = new object();
        private Func<IInterval> _intervalGen;

        public ElasticsearchBatchPusher(IHttpClient httpClient, IConfigurationValueProvider configurationValueProvider, string esUrl, IIndexNamer indexNamer, int batchSize = 500)
        {
            _indexNamer = indexNamer;
            _httpClient = httpClient;
            _batchSize = batchSize;
            _esUrl = esUrl;

            if(!bool.TryParse(configurationValueProvider.GetValue(ConfigurationKeys.EsPipelineEnabled), out _setPipeline))
                _setPipeline = false;

            var esBackOffMinSecondsString = configurationValueProvider.GetValue(ConfigurationKeys.EsBackOffMinSeconds);
            var esBackOffMaxSecondsString = configurationValueProvider.GetValue(ConfigurationKeys.EsBackOffMaxSeconds);
            var esBackOffMinSeconds = string.IsNullOrWhiteSpace(esBackOffMinSecondsString) ? 5 : int.Parse(esBackOffMinSecondsString);
            var esBackOffMaxSeconds = string.IsNullOrWhiteSpace(esBackOffMaxSecondsString) ? 100 : int.Parse(esBackOffMaxSecondsString);
            _intervalGen = () => new DoublyIncreasingInterval(TimeSpan.FromSeconds(esBackOffMinSeconds),
                TimeSpan.FromSeconds(esBackOffMaxSeconds), 5);
        }

        private static async Task PushbatchAsync(Batch batch, IHttpClient client, string esUrl, IInterval interval)
        {

            if (batch.Count == 0)
                return;

            try
            {
                int retry = 0;
                List<int> statuses = null;
                string content = string.Empty;
                string reqContent = string.Empty;
                do
                {
                    reqContent = batch.ToString();
                    var responseMessage = await client.PostAsync(esUrl + "_bulk",
                        new StringContent(reqContent, Encoding.UTF8, "application/json"));
                    content = responseMessage.Content == null ? "" :
                        (await responseMessage.Content.ReadAsStringAsync());

                    if(!responseMessage.IsSuccessStatusCode)
                            throw new ApplicationException(string.Format("Unsuccessful ES bulk: {0} - {1}", responseMessage.StatusCode, content));

                    dynamic j = JObject.Parse(content);
                    if (j == null || j.items == null)
                        throw new ApplicationException(string.Format("Unsuccessful ES bulk - items null: {0}", content));

                    var items = (JArray) j.items;
                    statuses = items.Children<JObject>().Select(x => x.Properties().First().Value["status"].Value<int>()).ToList();

                    if (statuses.Any(y => y < 200 || (y > 299 && y != 429)))
                    {
                       TheTrace.TraceWarning("LOOK!! We had some errors from ES bulk at retry {1}: {0}", content, retry);
                    }

                    if (statuses.Any(y => y == 429))
                    {
                        var timeSpan = interval.Next();
                        TheTrace.TraceWarning("LOOK!! Got 429 -> backing off for {0} seconds", timeSpan.TotalSeconds);
                        Thread.Sleep(timeSpan);
                    }
                    else
                    {
                        interval.Reset();
                    }

                    TheTrace.TraceInformation("ConveyorBelt_Pusher: Pushing {1} records to {0} [retry: {2}]", esUrl, batch.Count, retry);

                } while ((batch = Batch.Prune(batch, statuses, content, reqContent)) != null && batch.Count > 0 && retry++ < 3);

                if (batch.Count > 0)
                {
                    TheTrace.TraceWarning("WARNING!!! Some residual documents could not be inserted even after retries: {0}", batch.Count);
                    batch.Clear();
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
            if (!string.IsNullOrEmpty(source.Filter))
            {
                _filters.AddOrUpdate(source.Filter, new SimpleFilter(source.Filter), ((s, filter) => filter));
                if (!_filters[source.Filter].Satisfies(entity))
                    return;
            }


            var op = _setPipeline
                ? new {
                    index = new
                    {
                        _index = source.IndexName ?? _indexNamer.BuildName(entity.Timestamp, source.DynamicProperties["MappingName"].ToString().ToLowerInvariant()),
                        _type = source.DynamicProperties["MappingName"].ToString(),
                        _id = entity.PartitionKey + entity.RowKey,
                        pipeline = source.DynamicProperties["MappingName"].ToString().ToLowerInvariant()
                    }
                }
                : (object) new
                {
                    index = new
                    {
                        _index = source.IndexName ?? _indexNamer.BuildName(entity.Timestamp, source.DynamicProperties["MappingName"].ToString().ToLowerInvariant()),
                        _type = source.DynamicProperties["MappingName"].ToString(),
                        _id = entity.PartitionKey + entity.RowKey,
                    }
                };

            var doc = new JObject();
            doc.Add("@timestamp", entity.GetTimestamp(source));
            doc.Add("PartitionKey", entity.PartitionKey);
            doc.Add("RowKey", entity.RowKey);
            doc.Add("cb_type", source.TypeName);

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
                Batch batch = null;
                lock (_lock)
                {
                    if (_batch.Count >= _batchSize)
                    {
                        batch = _batch.CloneAndClear();
                    }
                }

                if( batch != null)
                {
                    await PushbatchAsync(batch, _httpClient, _esUrl, _intervalGen());
                    TheTrace.TraceInformation("ConveyorBelt_Pusher: Pushed records to ElasticSearch for {0}-{1}",
                        source.PartitionKey,
                        source.RowKey);
                }
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
            return PushbatchAsync(_batch.CloneAndClear(), _httpClient, _esUrl, _intervalGen());
        }

        internal class Batch
        {
            private ConcurrentBag<Tuple<string, string>> _list = new ConcurrentBag<Tuple<string, string>>();
            private object _lock = new object();

            public Batch()
            {
                
            }

            private Batch(ConcurrentBag<Tuple<string, string>> list)
            {
                _list = list;
            }

            public void AddDoc(string op, string doc)
            {
                if(op == null || doc == null)
                    throw new ArgumentNullException("Watchout ! op+doc");

                lock (_lock)
                {
                    _list.Add(new Tuple<string, string>(op, doc));
                }
            }

            public int Count
            {
                get { return _list.Count; }
            }

            public void Clear()
            {
                _list = new ConcurrentBag<Tuple<string, string>>();
            }

            public override string ToString()
            {
                var sb = new StringBuilder();
                var copy = _list.ToArray();
                foreach (var item in copy)
                {
                    sb.Append(item.Item1);
                    sb.Append('\n');
                    sb.Append(item.Item2);
                    sb.Append('\n');
                }

                return sb.ToString();
            }

            public static Batch Prune(Batch batch, IList<int> statuses, string contentInfo = null, string reqContentInfo = null)
            {
                if(statuses.Count != batch._list.Count)
                    throw new InvalidOperationException(string.Format(
                        "Statuses should have exactly the same number of items. {0} vs. statuses {1}\r\n{2}", batch._list.Count, statuses.Count, contentInfo ?? string.Empty));

                var list = new ConcurrentBag<Tuple<string, string>>();

                int i = 0;
                foreach (var item in batch._list)
                {
                    var si = statuses[i];
                    if (si >= 300) // if success
                        list.Add(item);
                    i++;
                }
              
                
                return new Batch(list);
            }

            public Batch CloneAndClear()
            {
                Tuple<string, string>[] copy;
                lock (_lock)
                {
                    copy = _list.ToArray();
                    this._list = new ConcurrentBag<Tuple<string, string>>();
                }
                
                return new Batch(new ConcurrentBag<Tuple<string, string>>(copy));
            }
        }
    }
}
