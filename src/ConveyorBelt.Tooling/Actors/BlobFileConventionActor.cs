using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Parsing;
using ConveyorBelt.Tooling.Telemetry;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using PerfIt;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("BlobFileScheduled-Process", 6)]
    public class BlobFileConventionActor : IProcessorActor
    {
        private readonly IElasticsearchBatchPusher _pusher;
        private readonly ITempDownloadLocationProvider _tempDownloadLocationProvider;
        private readonly ITelemetryProvider _telemetryProvider;
        private readonly SimpleInstrumentor _durationInstrumentor;

        public BlobFileConventionActor(IElasticsearchBatchPusher pusher, 
                                       ITempDownloadLocationProvider tempDownloadLocationProvider,
                                       ITelemetryProvider telemetryProvider)
        {
            _tempDownloadLocationProvider = tempDownloadLocationProvider;
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
               blobFileScheduled.Source.RowKey);

            await _durationInstrumentor.InstrumentAsync(async () =>
            {
                var account = CloudStorageAccount.Parse(blobFileScheduled.Source.ConnectionString);
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
                    if (currentLength == blobFileScheduled.LastPosition)
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
                        var stream = await DownloadToFileAsync(mainBlob);
                        try
                        {
                            currentLength = stream.Length;
                            var parser = FactoryHelper.Create<IParser>(blobFileScheduled.Source.DynamicProperties["Parser"].ToString(), typeof(IisLogParser));
                            var hasAnything = false;
                            var minDateTime = DateTimeOffset.MaxValue;
                            foreach (var entity in parser.Parse(stream, mainBlob.Uri, blobFileScheduled.LastPosition))
                            {
                                await _pusher.PushAsync(entity, blobFileScheduled.Source);
                                hasAnything = true;
                                minDateTime = minDateTime > entity.Timestamp ? entity.Timestamp : minDateTime;
                            }

                            if (hasAnything)
                            {
                                await _pusher.FlushAsync();
                                TheTrace.TraceInformation("BlobFileConventionActor - pushed records for {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
                                _telemetryProvider.WriteTelemetry(
                                    "BlobFileConventionActor log delay duration",
                                    (long)(DateTimeOffset.UtcNow - minDateTime).TotalMilliseconds, 
                                    blobFileScheduled.Source.RowKey);
                            }
                        }
                        finally
                        {
                            stream.Close();
                            File.Delete(stream.Name);
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

        private async Task<FileStream> DownloadToFileAsync(CloudBlockBlob blob)
        {
            var downloadFolder = _tempDownloadLocationProvider.GetDownloadFolder();
            string fileName = Path.Combine(downloadFolder, Guid.NewGuid().ToString("N"));
            var fileStream = new FileStream(fileName, FileMode.Create);
            await blob.DownloadToStreamAsync(fileStream);
            fileStream.Position = 0;
            return fileStream;
        }
    }
}
