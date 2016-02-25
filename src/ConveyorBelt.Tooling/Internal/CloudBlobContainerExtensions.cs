using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConveyorBelt.Tooling.Internal
{
    public static class CloudBlobContainerExtensions
    {
        public static CloudBlockBlob GetBlobReferenceWithAlternateName(this CloudBlobContainer container,  string id, Func<string, string> alternateNamer,
            out bool exists)
        {
            var alternateId = alternateNamer(id);
            var blob = container.GetBlockBlobReference(id);
            var altblob = container.GetBlockBlobReference(alternateId);
            var blobExists = blob.Exists();
            var altblobExists = altblob.Exists();
            exists = blobExists || altblobExists;
            return altblobExists ? altblob : blob;
        }
    }
}
