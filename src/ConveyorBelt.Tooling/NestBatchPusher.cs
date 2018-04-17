using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BeeHive.Configuration;
using ConveyorBelt.Tooling.Internal;
using Elasticsearch.Net;
using Microsoft.WindowsAzure.Storage.Table;
using Nest;

namespace ConveyorBelt.Tooling
{
    public class NestBatchPusher
    {
        private readonly IIndexNamer _indexNamer;
        private readonly IElasticClient _client;
        private readonly TimeSpan _backOffTime;
        private readonly int _batchSize;
        private readonly bool _setPipeline;

        public NestBatchPusher(string esUrl, IIndexNamer indexNamer, IConfigurationValueProvider configurationValueProvider, int batchSize = 100)
        {
            _indexNamer = indexNamer;
            _batchSize = batchSize;

            var endodedCreds = configurationValueProvider.GetValue(ConfigurationKeys.TabSeparatedCustomEsHttpHeaders)
                .Replace("Authorization: Basic", "").Trim();
            var credentials = Encoding.UTF8.GetString(Convert.FromBase64String(endodedCreds)).Split(':');

            if(!bool.TryParse(configurationValueProvider.GetValue(ConfigurationKeys.EsPipelineEnabled), out _setPipeline))
                _setPipeline = false;

            var connectionConfiguration = new ConnectionSettings(
                    new SingleNodeConnectionPool(new Uri(esUrl)))
                .ServerCertificateValidationCallback(delegate { return true; })
                .BasicAuthentication(credentials[0], credentials[1])
                .RequestTimeout(TimeSpan.FromMinutes(2))
                //.EnableDebugMode(_ => { if(!_.Success) Console.WriteLine(); })
                .DefaultIndex("DafaultIndex");

            _client = new ElasticClient(connectionConfiguration);

            var backoffSeconds = configurationValueProvider.GetValue(ConfigurationKeys.EsBackOffMinSeconds);
            _backOffTime = TimeSpan.FromSeconds(string.IsNullOrWhiteSpace(backoffSeconds) ? 5 : int.Parse(backoffSeconds));
        }

        private string GetIndexName(DiagnosticsSourceSummary source, DateTimeOffset? timestamp)
        {
            return source.IndexName ?? _indexNamer.BuildName(timestamp, source.DynamicProperties["MappingName"].ToString().ToLowerInvariant());
        }

        public async Task<int> PushAll(IEnumerable<IDictionary<string, string>> lazyEnumerable, DiagnosticsSourceSummary source)
        {
            return await PushAllImpl(
                lazyEnumerable,
                GetIndexName(source, source.LastTimeOffset),
                source.DynamicProperties["MappingName"].ToString()
            ).ConfigureAwait(false);
        }

        public async Task<int> PushAll(IEnumerable<DynamicTableEntity> lazyEnumerable, DiagnosticsSourceSummary source)
        {
            return await PushAllImpl(
                lazyEnumerable.Select(entity => entity.ToDictionary(source)),
                GetIndexName(source, source.LastTimeOffset),
                source.DynamicProperties["MappingName"].ToString()
            ).ConfigureAwait(false);
        }

        private async Task<int> PushAllImpl(IEnumerable<IDictionary<string, string>> lazyEnumerable, string index, string mappingName)
        {
            var seenPages = 0;
            var tcs = new TaskCompletionSource<int>();

            var observableBulk = _client.BulkAll(lazyEnumerable, bulkDescriptor => {
                bulkDescriptor
                    .BufferToBulk((x, batch) => x.IndexMany(batch, (bd, d) => bd.Id(d["PartitionKey"] + d["RowKey"])))
                    .Index(index)
                    .Type(mappingName);

                if (_setPipeline)
                    bulkDescriptor.Pipeline(mappingName.ToLower());

                return bulkDescriptor
                    .MaxDegreeOfParallelism(10)
                    .Size(_batchSize)
                    .BackOffTime(_backOffTime);
            });

            var observer = new BulkAllObserver(
                onNext: (b) => Interlocked.Increment(ref seenPages),
                onCompleted: () => tcs.SetResult(seenPages),
                onError: e => tcs.SetException(e));

            observableBulk.Subscribe(observer);
            return await tcs.Task.ConfigureAwait(false);
        }
    }
}