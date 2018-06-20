﻿using System;
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
using Microsoft.WindowsAzure.Storage.Auth;
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
            //var account = CloudStorageAccount.Parse(source.ConnectionString);
            CloudStorageAccount account;
            if (!String.IsNullOrWhiteSpace(source.AccountSasKey))
            {
                // Create new storage credentials using the SAS token.
                var accountSas = new StorageCredentials(source.AccountSasKey);
                // Use these credentials and the account name to create a Blob service client.
                account = new CloudStorageAccount(accountSas,  source.AccountName, "", useHttps: true);
            }
            else
            {
                account = CloudStorageAccount.Parse(source.ConnectionString);
            }
            var client = account.CreateCloudBlobClient();
            var blobPath = source.GetProperty<string>("BlobPath");
            TheTrace.TraceInformation("IisBlobScheduler - pathformat: {0}", blobPath);
            blobPath = blobPath.TrimEnd('/') + "/"; // ensure path ends with /
            var offset = FileOffset.Parse(source.LastOffsetPoint);
            if (offset == null)
                throw new InvalidOperationException("FileOffset failed parsing: => " + source.LastOffsetPoint);

            DateTimeOffset maxOffset = offset.TimeOffset;
            FileOffset newOffset = null;
            var events = new List<Event>();

            foreach (var blob in client.ListBlobs(blobPath).Where(itm => itm is CloudBlockBlob)
                    .Cast<CloudBlockBlob>().OrderBy(x => x.Properties.LastModified))
            {
                if (blob.Properties.LastModified > offset.TimeOffset)
                {
                    var filename = blob.Uri.ToString();
                    newOffset = new FileOffset(filename, blob.Properties.LastModified ?? GetDefaultLastOffset(), 0);
                    TheTrace.TraceInformation("IisBlobScheduler - found {0}", blob.Uri);

                    events.Add(new Event(new BlobFileArrived()
                    {
                        Source = source.ToSummary(),
                        BlobId = filename,
                        Position = 0,
                        EndPosition = blob.Properties.Length
                    }));

                    TheTrace.TraceInformation("Created BlobFileArrived for file: {0}", filename);
                }
            }

            source.LastOffsetPoint = newOffset == null ? offset.ToString() : newOffset.ToString();
            return Task.FromResult((IEnumerable<Event>)events);
        }

        public SimpleBlobScheduler(IConfigurationValueProvider configurationValueProvider) 
            : base(configurationValueProvider)
        {
        }
    }
}
