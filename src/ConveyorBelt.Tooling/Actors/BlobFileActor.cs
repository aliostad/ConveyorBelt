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
using ConveyorBelt.Tooling.Parsing;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConveyorBelt.Tooling.Actors
{
    [ActorDescription("BlobFileArrived-Process", 5)]
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
            var account = CloudStorageAccount.Parse(blobFileArrived.Source.ConnectionString);
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(blobFileArrived.Source.Properties["ContainerName"].StringValue);
            var blob = container.GetBlockBlobReference(blobFileArrived.BlobId);
            var stream = new MemoryStream();
            await blob.DownloadToStreamAsync(stream);
            var parserTypeName = blobFileArrived.Source.Properties["Parser"].StringValue;
            var parserType = Assembly.GetExecutingAssembly().GetType(parserTypeName) ?? Type.GetType(parserTypeName);
            if(parserType == null)
                throw new InvalidOperationException("Parser type was null: " + parserTypeName);
            var parser = (IParser) Activator.CreateInstance(parserType);
            bool hasAnything = false;

            foreach (var entity in parser.Parse(stream, blob.Uri))
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
