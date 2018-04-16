using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Parsing;
using ConveyorBelt.Tooling.Telemetry;
using ConveyorBelt.Tooling.Scheduling;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using PerfIt;
using Microsoft.WindowsAzure.Storage;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("BlobFileScheduled-Process", 6)]
    public class BlobFileConventionActor : IProcessorActor
    {
        private readonly NestBatchPusher _pusher;
        private readonly ITelemetryProvider _telemetryProvider;
        private readonly SimpleInstrumentor _durationInstrumentor;

        public BlobFileConventionActor(NestBatchPusher pusher, 
                                       ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;
            _pusher = pusher;
            _durationInstrumentor = telemetryProvider.GetInstrumentor<BlobFileConventionActor>();
        }

        public void Dispose()
        {
        }

        public async Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            var events = new List<Event>();
            var blobFileScheduled = evnt.GetBody<BlobFileScheduled>();

            _telemetryProvider.WriteTelemetry(
               "BlobFileScheduled receive message delay duration",
               (long)(DateTime.UtcNow - evnt.Timestamp).TotalMilliseconds, 
               blobFileScheduled.Source.TypeName);

            await _durationInstrumentor.InstrumentAsync(async () =>
            {
                //var account = CloudStorageAccount.Parse(blobFileScheduled.Source.ConnectionString);
                CloudStorageAccount account;
                if (!String.IsNullOrWhiteSpace(blobFileScheduled.Source.AccountSasKey))
                {
                    // Create new storage credentials using the SAS token.
                    var accountSas = new StorageCredentials(blobFileScheduled.Source.AccountSasKey);
                    // Use these credentials and the account name to create a Blob service client.
                    account = new CloudStorageAccount(accountSas, blobFileScheduled.Source.AccountName, "", useHttps: true);
                }
                else
                {
                    account = CloudStorageAccount.Parse(blobFileScheduled.Source.ConnectionString);
                }
                var client = account.CreateCloudBlobClient();
                var container = client.GetContainerReference("wad-iis-logfiles");

                bool nextBlobExists;
                bool previousBlobExists;
                bool mainBlobExists;
                Func<string, string> alternater = s => s.Replace(".log", "_x.log");
                container.GetBlobReferenceWithAlternateName(blobFileScheduled.NextFile, alternater, out nextBlobExists);
                container.GetBlobReferenceWithAlternateName(blobFileScheduled.PreviousFile, alternater, out previousBlobExists);
                var mainBlob = container.GetBlobReferenceWithAlternateName(blobFileScheduled.FileToConsume, alternater, out mainBlobExists);

                if (!previousBlobExists && !mainBlobExists)
                {
                    TheTrace.TraceInformation("BlobFileConventionActor - previous blob does not exist. Stopped chasing {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
                    return; // will never be here. Stop chasing it.                
                }

                long currentLength = 0;
                if (mainBlobExists)
                {
                    currentLength = mainBlob.Properties.Length;
                    if (currentLength <= blobFileScheduled.LastPosition)
                    {
                        if (nextBlobExists)
                        {
                            TheTrace.TraceInformation("BlobFileConventionActor - Next blob exists. Stopped chasing {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
                            return; // done and dusted. Stop chasing it.
                        }

                        if (blobFileScheduled.StopChasingAfter < DateTimeOffset.Now)
                        {
                            TheTrace.TraceInformation("BlobFileConventionActor - Chase time past. Stopped chasing {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
                            return; // Stop chasing it.
                        }
                    }
                    else
                    {
                        using (var stream = mainBlob.OpenRead())
                        {
                            var parser = FactoryHelper.Create<IParser>(blobFileScheduled.Source.DynamicProperties["Parser"].ToString(), typeof(IisLogParser));
                            var minDateTime = DateTimeOffset.UtcNow;

                            var parsedRecords = parser.Parse(stream, mainBlob.Uri, blobFileScheduled.Source, blobFileScheduled.LastPosition);
                            var pages = await _pusher.PushAll(parsedRecords, blobFileScheduled.Source).ConfigureAwait(false);
                            
                            if (pages > 0)
                            {
                                TheTrace.TraceInformation("BlobFileConventionActor - pushed records for {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
                                _telemetryProvider.WriteTelemetry(
                                    "BlobFileConventionActor log delay duration",
                                    (long)(DateTimeOffset.UtcNow - minDateTime).TotalMilliseconds, 
                                    blobFileScheduled.Source.TypeName);
                            }

                            currentLength += stream.Position;
                        }
                    }
                }
                else
                {
                    if (blobFileScheduled.StopChasingAfter < DateTimeOffset.Now)
                    {
                        TheTrace.TraceInformation("BlobFileConventionActor - Chase time past. Stopped chasing {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
                        return; // Stop chasing it.
                    }
                }

                blobFileScheduled.LastPosition = currentLength;

                // let's defer
                events.Add(new Event(blobFileScheduled)
                {
                    EnqueueTime = DateTimeOffset.Now.Add(TimeSpan.FromSeconds(30))
                });

                TheTrace.TraceInformation("BlobFileConventionActor - deferred processing {0}. Length => {1}  at {2}", blobFileScheduled.FileToConsume, currentLength, DateTimeOffset.Now);
            });

            return events;
        }
    }
}
