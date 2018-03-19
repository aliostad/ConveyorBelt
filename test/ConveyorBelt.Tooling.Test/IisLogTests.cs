using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive.DataStructures;
using ConveyorBelt.Tooling.Parsing;
using Microsoft.WindowsAzure.Storage.Table;
using Moq;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class IisLogTests
    {
        [Fact]
        public void TestDataFile_ExtractsAllRecords()
        {
            var stream = new MemoryStream(File.ReadAllBytes("IisLog.txt"));
            var parser = new IisLogParser();
            var entities = parser.Parse(stream, new Uri("http://shipish/e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log", UriKind.Absolute), new DiagnosticsSourceSummary()).ToArray();
            Assert.Equal(entities[0]["s-computername"], "RD00155D4A0E2E");
            Assert.Equal(entities[0]["cs-method"], "GET");
            Assert.Equal(entities[1]["cs-uri-stem"], "/product/catalogue/v2/productgroups/ctl/4650127");
        }
    }
}
