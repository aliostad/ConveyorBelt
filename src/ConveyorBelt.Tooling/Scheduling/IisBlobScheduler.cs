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
    public class IisBlobScheduler : BaseScheduler
    {
        protected override Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            TheTrace.TraceInformation("IisBlobScheduler - Starting scheduling");
            var account = CloudStorageAccount.Parse(source.ConnectionString);
            var client = account.CreateCloudBlobClient();
            var pathFormat = source.GetProperty<string>("BlobPathFormat");
            TheTrace.TraceInformation("IisBlobScheduler - pathformat: {0}", pathFormat);
            pathFormat = pathFormat.TrimEnd('/') + "/"; // ensure path ends with /

            var offset = FileOffset.Parse(source.LastOffsetPoint);
            if(offset == null)
                throw new InvalidOperationException("FileOffset failed parsing: => " + source.LastOffsetPoint);
            int instanceIndex = 0;
            DateTimeOffset maxOffset = offset.TimeOffset;
            FileOffset newOffset = null;
            var events = new List<Event>();
            while (true)
            {
                bool found = false;

                var path = string.Format(pathFormat, instanceIndex);
                var isSingleInstance = path == pathFormat;


                TheTrace.TraceInformation("IisBlobScheduler - Looking into {0}", path);
                foreach (var blob in client.ListBlobs(path).Where(itm => itm is CloudBlockBlob)
                    .Cast<CloudBlockBlob>().OrderBy(x => x.Properties.LastModified))
                {
                    if (blob.Properties.LastModified > offset.TimeOffset)
                    {
                        var filename = blob.Uri.ToString();
                        if (!found) // first time running
                        {
                            newOffset = new FileOffset(filename, 
                                blob.Properties.LastModified ?? DateTimeOffset.UtcNow, blob.Properties.Length);
                        }

                        TheTrace.TraceInformation("IisBlobScheduler - found {0}", blob.Uri);
                        found = true;
                        events.Add(new Event(new BlobFileArrived()
                        {
                            Source = source.ToSummary(), 
                            BlobId = filename,
                            Position = (filename == offset.FileName) ? offset.Position : 0, // if same file then pass offset
                            EndPosition = blob.Properties.Length
                        }));
                    }
                }

                if (!found || isSingleInstance)
                {
                    TheTrace.TraceInformation("IisBlobScheduler - Breaking out with index of {0}", instanceIndex);
                    break;
                }

                instanceIndex++;
            }

            source.LastOffsetPoint = newOffset == null ? offset.ToString() : newOffset.ToString();
            return Task.FromResult((IEnumerable<Event>) events);
        }

        public IisBlobScheduler(ILockStore lockStore, IConfigurationValueProvider configurationValueProvider)
            : base(configurationValueProvider)
        {
        }
    }
}
