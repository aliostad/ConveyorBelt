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
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("BlobFileScheduled-Process", 6)]
    public class BlobFileConventionActor : IProcessorActor
    {
        private IElasticsearchBatchPusher _pusher;

        public BlobFileConventionActor(IElasticsearchBatchPusher pusher)
        {
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
            var nextBlob = container.GetBlockBlobReference(blobFileScheduled.NextFile);
            var previousBlob = container.GetBlockBlobReference(blobFileScheduled.PreviousFile);
            var mainBlob = container.GetBlockBlobReference(blobFileScheduled.FileToConsume);
            var nextBlobExists = await nextBlob.ExistsAsync();
            var previousBlobExists = await previousBlob.ExistsAsync();
            var mainBlobExists = await mainBlob.ExistsAsync();

            if (blobFileScheduled.StopChasingAfter < DateTimeOffset.Now)
            {
                TheTrace.TraceInformation("BlobFileConventionActor - Chase time past. Stopped chasing {0}", blobFileScheduled.FileToConsume);
                return events; // Stop chasing it.
            }

            if (!previousBlobExists && !mainBlobExists)
            {
                TheTrace.TraceInformation("BlobFileConventionActor - previous blob does not exist. Stopped chasing {0}", blobFileScheduled.FileToConsume);
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
                        TheTrace.TraceInformation("BlobFileConventionActor - Next blob exists. Stopped chasing {0}", blobFileScheduled.FileToConsume);
                        return events; // done and dusted. Stop chasing it.
                    }
                }
                else
                {
                    var stream = new MemoryStream();
                    await mainBlob.DownloadToStreamAsync(stream);
                    currentLength = stream.Length;
                    var parser = FactoryHelper.Create<IParser>(blobFileScheduled.Source.DynamicProperties["Parser"].ToString(), typeof(IisLogParser));
                    bool hasAnything = false;

                    foreach (var entity in parser.Parse(stream, mainBlob.Uri, blobFileScheduled.LastPosition))
                    {
                        await _pusher.PushAsync(entity, blobFileScheduled.Source);
                        hasAnything = true;
                    }

                    if (hasAnything)
                    {
                        await _pusher.FlushAsync();
                        TheTrace.TraceInformation("BlobFileConventionActor - pushed records for {0}", blobFileScheduled.FileToConsume);
                    }
                }
            }

            blobFileScheduled.LastPosition = currentLength;

            // let's defer
            events.Add(new Event(blobFileScheduled)
            {
                EnqueueTime = DateTimeOffset.Now.Add(TimeSpan.FromSeconds(30))
            });

            TheTrace.TraceInformation("BlobFileConventionActor - deferred processing {0}. Length => {1}", blobFileScheduled.FileToConsume, currentLength);

            return events;
        }
    }
}
