using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Azure;
using ConveyorBelt.Tooling.Events;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Parsing;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("BlobFileArrived-Process", 3)]
    public class BlobFileActor : IProcessorActor
    {
        private IElasticsearchBatchPusher _pusher;

        public BlobFileActor(IElasticsearchBatchPusher pusher)
        {
            _pusher = pusher;
        }

        public void Dispose()
        {
        }

        public async Task<IEnumerable<Event>> ProcessAsync(Event evnt)
        {
            var blobFileArrived = evnt.GetBody<BlobFileArrived>();
            TheTrace.TraceInformation("Got {0} from {1}", blobFileArrived.BlobId,
                 blobFileArrived.Source.TypeName);
            var account = CloudStorageAccount.Parse(blobFileArrived.Source.ConnectionString);
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(blobFileArrived.Source.DynamicProperties["ContainerName"].ToString());
            var uri = new Uri(blobFileArrived.BlobId);
            var id = string.Join("", uri.Segments.Skip(2));
            var blob = container.GetBlockBlobReference(id);
            if(!blob.Exists())
                throw new InvalidOperationException("Blob does not exist: " + id);
            var stream = new MemoryStream();
            await blob.DownloadToStreamAsync(stream);          
            var parser = FactoryHelper.Create<IParser>(blobFileArrived.Source.DynamicProperties["Parser"].ToString(), typeof(IisLogParser));
            bool hasAnything = false;

            foreach (var entity in parser.Parse(stream, blob.Uri, blobFileArrived.Position ?? 0, blobFileArrived.EndPosition ?? 0))
            {
                await _pusher.PushAsync(entity, blobFileArrived.Source);
                hasAnything = true;
            }

            if (hasAnything)
            {
                await _pusher.FlushAsync();
            }

            return Enumerable.Empty<Event>();
        }
    }
}
