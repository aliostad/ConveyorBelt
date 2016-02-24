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
using ConveyorBelt.Tooling.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class IisBlobConventionScheduler : BaseScheduler
    {
        protected override async Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            const string DefaultIisLogFileFormatConvention = "u_exyyMMddHH";

            TheTrace.TraceInformation("IisBlobConventionScheduler - Starting scheduling");
            var account = CloudStorageAccount.Parse(source.ConnectionString);
            var client = account.CreateCloudBlobClient();
            var pathFormat = source.GetProperty<string>("BlobPathFormat");
            TheTrace.TraceInformation("IisBlobConventionScheduler - pathformat: {0}", pathFormat);
            pathFormat = pathFormat.TrimEnd('/') + "/"; // ensure path ends with /
            var iisLogFileFormatConvention = source.GetProperty<string>("IisLogFileFormatConvention") ?? DefaultIisLogFileFormatConvention;

            var offset = FileOffset.Parse(source.LastOffsetPoint);
            if (offset == null)
                throw new InvalidOperationException("FileOffset failed parsing: => " + source.LastOffsetPoint);

            int instanceIndex = 0;
            
            var nextOffset = DateTimeOffset.Now;
            var events = new List<Event>();
            var fullNumberOfHoursInBetween = offset.TimeOffset.GetFullNumberOfHoursInBetween(nextOffset);
            if (fullNumberOfHoursInBetween == 0)
                return events;

            while (true)
            {
                bool found = false;
                var path = string.Format(pathFormat, instanceIndex);
                instanceIndex++;
                TheTrace.TraceInformation("IisBlobConventionScheduler - Looking into {0}", path);
                var any = client.ListBlobs(path).Any(itm => itm is CloudBlockBlob);
                if (!any)
                    break;
                
                for (int i = 1; i < fullNumberOfHoursInBetween+1; i++)
                {
                    var fileToConsume = string.Format(iisLogFileFormatConvention, offset.TimeOffset.AddHours(i).UtcDateTime) + ".log";
                    var previousFile = string.Format(iisLogFileFormatConvention, offset.TimeOffset.AddHours(i - 1).UtcDateTime) + ".log";
                    events.Add(new Event(new BlobFileScheduled()
                    {
                        FileToConsume = path.Replace("wad-iis-logfiles/", "") + fileToConsume,
                        PreviousFile = path.Replace("wad-iis-logfiles/", "") + previousFile,
                        Source = source.ToSummary(),
                        StopChasingAfter = DateTimeOffset.Now.Add(TimeSpan.FromMinutes(80))
                    }));

                    TheTrace.TraceInformation("IisBlobConventionScheduler - Scheduled Event: {0}", fileToConsume);
                }
            }

            source.LastOffsetPoint = new FileOffset(string.Empty, nextOffset).ToString();
            return events;
        }

        public IisBlobConventionScheduler(ILockStore lockStore, IConfigurationValueProvider configurationValueProvider)
            : base(lockStore, configurationValueProvider)
        {
        }
    }
}
