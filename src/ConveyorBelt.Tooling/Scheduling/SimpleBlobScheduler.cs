using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using BeeHive.DataStructures;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Events;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConveyorBelt.Tooling.Scheduling
{
    /// <summary>
    /// This one just looks in a folder and reads latest stuff
    /// </summary>
    public class SimpleBlobScheduler : BaseScheduler
    {
        protected override Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            TheTrace.TraceInformation("IisBlobScheduler - Starting scheduling");
            var account = CloudStorageAccount.Parse(source.ConnectionString);
            var client = account.CreateCloudBlobClient();
            var blobPath = source.GetProperty<string>("BlobPath");
            TheTrace.TraceInformation("IisBlobScheduler - pathformat: {0}", blobPath);
            blobPath = blobPath.TrimEnd('/') + "/"; // ensure path ends with /
            var offset = FileOffset.Parse(source.LastOffsetPoint);
            if (offset == null)
                throw new InvalidOperationException("FileOffset failed parsing: => " + source.LastOffsetPoint);
            int instanceIndex = 0;
            DateTimeOffset maxOffset = offset.TimeOffset;
            FileOffset newOffset = null;
            var events = new List<Event>();

            foreach (var blob in client.ListBlobs(blobPath).Where(itm => itm is CloudBlockBlob)
                    .Cast<CloudBlockBlob>().OrderByDescending(x => x.Properties.LastModified))
            {
                if (blob.Properties.LastModified > offset.TimeOffset)
                {
                    var filename = blob.Uri.ToString();
                    newOffset = new FileOffset(filename, blob.Properties.LastModified ?? DateTimeOffset.UtcNow, 0);
                    TheTrace.TraceInformation("IisBlobScheduler - found {0}", blob.Uri);

                    events.Add(new Event(new BlobFileArrived()
                    {
                        Source = source.ToSummary(),
                        BlobId = filename,
                        Position = (filename == offset.FileName) ? offset.Position : 0, // if same file then pass offset
                        EndPosition = blob.Properties.Length
                    }));
                }
            }

            source.LastOffsetPoint = newOffset == null ? offset.ToString() : newOffset.ToString();
            return Task.FromResult((IEnumerable<Event>)events);
        }

        public SimpleBlobScheduler(ILockStore lockStore, IConfigurationValueProvider configurationValueProvider) : base(lockStore, configurationValueProvider)
        {
        }
    }
}
