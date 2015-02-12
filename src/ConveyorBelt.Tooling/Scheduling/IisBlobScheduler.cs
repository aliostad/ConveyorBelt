using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.DataStructures;
using ConveyorBelt.Tooling.Events;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class IisBlobScheduler : BaseScheduler
    {
        protected async override Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            TheTrace.TraceInformation("IisBlobScheduler - Starting scheduling");
            var account = CloudStorageAccount.Parse(source.ConnectionString);
            var client = account.CreateCloudBlobClient();
            var pathFormat = source.GetProperty<string>("BlobPathFormat");
            TheTrace.TraceInformation("IisBlobScheduler - pathformat: {0}", pathFormat);

            var offset = DateTimeOffset.Parse(source.LastOffsetPoint);
            int instanceIndex = 0;
            DateTimeOffset maxOffset = offset;
            var events = new List<Event>();
            while (true)
            {
                bool found = false;
                var path = string.Format(pathFormat, instanceIndex);
                TheTrace.TraceInformation("IisBlobScheduler - Looking into {0}", path);
                foreach (var itm in client.ListBlobs(path))
                {
                    var blob = itm as CloudBlockBlob;
                    if (blob != null && blob.Properties.LastModified > offset)
                    {
                        TheTrace.TraceInformation("IisBlobScheduler - found {0}", blob.Uri);
                        found = true;
                        maxOffset = offset > blob.Properties.LastModified.Value
                            ? offset
                            : blob.Properties.LastModified.Value;
                        events.Add(new Event(new BlobFileArrived() { Source = source.ToSummary(), BlobId = blob.Uri.ToString() }));
                    }
                }

                if (!found)
                {
                    TheTrace.TraceInformation("IisBlobScheduler - Breaking out with index of {0}", instanceIndex);
                    break;
                }
                instanceIndex++;
            }

            source.LastOffsetPoint = maxOffset.ToString("O");
            return events;
        }

        public IisBlobScheduler(ILockStore lockStore) : base(lockStore)
        {
        }
    }
}
