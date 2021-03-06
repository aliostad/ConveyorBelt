﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Parsing;
using ConveyorBelt.Tooling.Scheduling;
using ConveyorBelt.Tooling.Telemetry;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using PerfIt;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("BlobFileArrived-Process", 6)]
    public class BlobFileActor : IProcessorActor
    {
        private readonly ITelemetryProvider _telemetryProvider;
        private readonly SimpleInstrumentor _durationInstrumentor;
        private readonly NestBatchPusher _pusher;

        public BlobFileActor(NestBatchPusher pusher, ITelemetryProvider telemetryProvider)
        {
            _pusher = pusher;
            _telemetryProvider = telemetryProvider;
            _durationInstrumentor = telemetryProvider.GetInstrumentor<BlobFileActor>();
        }

        public void Dispose()
        {
        }

        public async Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            var blobFileArrived = evnt.GetBody<BlobFileArrived>();

            _telemetryProvider.WriteTelemetry(
                "BlobFileActor receive message delay duration",
                (long)(DateTime.UtcNow - evnt.Timestamp).TotalMilliseconds, 
                blobFileArrived.Source.TypeName);

            await _durationInstrumentor.InstrumentAsync(async () =>
            {
                TheTrace.TraceInformation("Got {0} from {1}", blobFileArrived.BlobId,
                 blobFileArrived.Source.TypeName);
                //var account = CloudStorageAccount.Parse(blobFileArrived.Source.ConnectionString);
                CloudStorageAccount account;
                if (!String.IsNullOrWhiteSpace(blobFileArrived.Source.AccountSasKey))
                {
                    // Create new storage credentials using the SAS token.
                    var accountSas = new StorageCredentials(blobFileArrived.Source.AccountSasKey);
                    // Use these credentials and the account name to create a Blob service client.
                    account = new CloudStorageAccount(accountSas, blobFileArrived.Source.AccountName, "", useHttps: true);
                }
                else
                {
                    account = CloudStorageAccount.Parse(blobFileArrived.Source.ConnectionString);
                }
                var client = account.CreateCloudBlobClient();
                var container = client.GetContainerReference(blobFileArrived.Source.DynamicProperties["ContainerName"].ToString());
                var uri = new Uri(blobFileArrived.BlobId);
                var id = string.Join("", uri.Segments.Skip(2));
                var blob = container.GetBlockBlobReference(id);
                if (!blob.Exists())
                    throw new InvalidOperationException("Blob does not exist: " + id);
                var stream = new MemoryStream();
                await blob.DownloadToStreamAsync(stream);
                var parser = FactoryHelper.Create<IParser>(blobFileArrived.Source.DynamicProperties["Parser"].ToString(), typeof(IisLogParser));
                var hasAnything = false;
                var minDateTime = DateTimeOffset.UtcNow;

                var records = parser.Parse(() => stream, blob.Uri, blobFileArrived.Source, new ParseCursor(blobFileArrived.Position ?? 0) {EndPosition = blobFileArrived.EndPosition ?? 0});
                var seenPages = await _pusher.PushAll(records, blobFileArrived.Source).ConfigureAwait(false);
                hasAnything = seenPages > 0;

                if (hasAnything)
                {
                    _telemetryProvider.WriteTelemetry(
                        "BlobFileActor message processing duration",
                        (long)(DateTimeOffset.UtcNow - minDateTime).TotalMilliseconds, 
                        blobFileArrived.Source.TypeName);
                }
            }, blobFileArrived.Source.TypeName);

            return Enumerable.Empty<Event>();
        }
    }
}
