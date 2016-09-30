using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Parsing;
using ConveyorBelt.Tooling.Scheduling;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("BlobFileScheduled-Process", 6)]
    public class BlobFileConventionActor : IProcessorActor
    {
        private IElasticsearchBatchPusher _pusher;
        private ITempDownloadLocationProvider _tempDownloadLocationProvider;

        public BlobFileConventionActor(IElasticsearchBatchPusher pusher, ITempDownloadLocationProvider tempDownloadLocationProvider)
        {
            _tempDownloadLocationProvider = tempDownloadLocationProvider;
            _pusher = pusher;
        }

        public void Dispose()
        {
            
        }

        public async Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            var events = new List<Event>();
            var blobFileScheduled = evnt.GetBody<BlobFileScheduled>();
            var account = CloudStorageAccount.Parse(blobFileScheduled.Source.ConnectionString);
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference("wad-iis-logfiles");
            
            bool nextBlobExists;
            bool previousBlobExists;
            bool mainBlobExists;
            Func<string, string> alternater = s => s.Replace(".log", "_x.log");
            var nextBlob = container.GetBlobReferenceWithAlternateName(blobFileScheduled.NextFile, alternater, out nextBlobExists);
            var previousBlob = container.GetBlobReferenceWithAlternateName(blobFileScheduled.PreviousFile, alternater, out previousBlobExists);
            var mainBlob = container.GetBlobReferenceWithAlternateName(blobFileScheduled.FileToConsume, alternater, out mainBlobExists);

            if (!previousBlobExists && !mainBlobExists)
            {
                TheTrace.TraceInformation("BlobFileConventionActor - previous blob does not exist. Stopped chasing {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
                return events; // will never be here. Stop chasing it.                
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
                        return events; // done and dusted. Stop chasing it.
                    }

                    if (blobFileScheduled.StopChasingAfter < DateTimeOffset.Now)
                    {
                        TheTrace.TraceInformation("BlobFileConventionActor - Chase time past. Stopped chasing {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
                        return events; // Stop chasing it.
                    }
                }
                else
                {
                    var stream = await DownloadToFileAsync(mainBlob);
                    try
                    {
                        currentLength = stream.Length;
                        var parser = FactoryHelper.Create<IParser>(blobFileScheduled.Source.DynamicProperties["Parser"].ToString(), typeof(IisBlobConventionScheduler));
                        bool hasAnything = false;

                        foreach (var entity in parser.Parse(stream, mainBlob.Uri, blobFileScheduled.LastPosition))
                        {
                            await _pusher.PushAsync(entity, blobFileScheduled.Source);
                            hasAnything = true;
                        }

                        if (hasAnything)
                        {
                            await _pusher.FlushAsync();
                            TheTrace.TraceInformation("BlobFileConventionActor - pushed records for {0} at {1}", blobFileScheduled.FileToConsume, DateTimeOffset.Now);
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
                    return events; // Stop chasing it.
                }

            }

            blobFileScheduled.LastPosition = currentLength;

            // let's defer
            events.Add(new Event(blobFileScheduled, new Dictionary<string, object> {{ConveyorBeltConstants.ElasticsearchTypeHeader, blobFileScheduled.Source.TypeName}})
            {
                EnqueueTime = DateTimeOffset.Now.Add(TimeSpan.FromSeconds(30))
            });

            TheTrace.TraceInformation("BlobFileConventionActor - deferred processing {0}. Length => {1}  at {2}", blobFileScheduled.FileToConsume, currentLength, DateTimeOffset.Now);

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
