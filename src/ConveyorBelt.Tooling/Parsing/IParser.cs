using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Parsing
{
    public interface IParser
    {
        IEnumerable<DynamicTableEntity> Parse(Stream body, Uri id, long position = 0, long endPosition = 0);
    }
}
