using System;
using System.IO;
using System.Linq;
using ConveyorBelt.Tooling.Parsing;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class IisLogTests
    {
        private const string SeventhRowKey = "u_ex15020701_1987";

        [Fact]
        public void TestDataFile_ExtractsAllRecords()
        {
            var stream = new MemoryStream(File.ReadAllBytes("IisLog.txt"));
            var parser = new IisLogParser();
            var entities = parser.Parse(stream, new Uri("http://shipish/e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log", UriKind.Absolute), new DiagnosticsSourceSummary()).ToArray();
            Assert.Equal("RD00155D4A0E2E", entities[0]["s-computername"]);
            Assert.Equal("GET", entities[0]["cs-method"]);
            Assert.Equal("store=AU", entities[0]["cs-uri-query"]);
            Assert.Equal("/product/catalogue/v2/productgroups/ctl/4650127", entities[1]["cs-uri-stem"]);
            Assert.Equal(SeventhRowKey, entities[6]["RowKey"]);
        }

        [Fact]
        public void TestDataFile_ExtractsAllRecordsInOffsetRange()
        {
            var stream = new MemoryStream(File.ReadAllBytes("IisLog.txt"));
            var parser = new IisLogParser();
            var entities = parser.Parse(
                stream,
                new Uri("http://shipish/e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log", UriKind.Absolute),
                new DiagnosticsSourceSummary(),
                504/*6th row*/, 691/*7th row*/).ToArray();

            Assert.Equal("RD00155D4A0E2E", entities[0]["s-computername"]);
            Assert.Equal("GET", entities[0]["cs-method"]);
            Assert.Equal("store=US", entities[0]["cs-uri-query"]);
        }

        [Fact]
        public void TestDataFile_ExtractsAllRecordsStartingFromOffsetWithChangingHeaders()
        {
            var stream = new MemoryStream(File.ReadAllBytes("IisLog.txt"));
            var parser = new IisLogParser();
            var entities = parser.Parse(
                stream,
                new Uri("http://shipish/e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log", UriKind.Absolute),
                new DiagnosticsSourceSummary(),
                1792/*15th row*/).ToArray();

            Assert.Equal("RD00155D4A0E2E", entities[0]["s-computername"]);
            Assert.Equal("GET", entities[0]["cs-method"]);
            Assert.Equal("store=US", entities[0]["cs-uri-query"]);
            Assert.Equal("/product/catalogue/v2/productgroups/ctl/5206173", entities[0]["cs-uri-stem"]);
            Assert.Equal("78", entities[0]["time-taken"]);
            Assert.Equal(SeventhRowKey, entities[0]["RowKey"]);
        }
    }
}