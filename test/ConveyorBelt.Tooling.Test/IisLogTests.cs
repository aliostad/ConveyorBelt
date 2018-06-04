using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ConveyorBelt.Tooling.Parsing;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class IisLogTests
    {
        private const string IisFileName = @"data\IisLog.txt";

        private static IDictionary<int, int> GetDynamicRowOffsets()
        {
            var text = Encoding.ASCII.GetString(File.ReadAllBytes(IisFileName));
            var offset = 0;
            var idx = 1;

            var res = new Dictionary<int, int> {{ idx++, offset }};
            while ((offset = text.IndexOf(Environment.NewLine, offset, StringComparison.InvariantCultureIgnoreCase)) > 0)
            {
                res.Add(idx++, offset += Environment.NewLine.Length);
            }

            return res;
        }

        private static readonly Lazy<IDictionary<int, int>> RowOffsetsLazy
            = new Lazy<IDictionary<int, int>>(GetDynamicRowOffsets);

        private static IDictionary<int, int> RowOffsets => RowOffsetsLazy.Value;

        private static string GetRowKey(int row) => $"u_ex15020701_{RowOffsets[row + 1]}";

        [Fact]
        public void TestDataFile_ExtractsAllRecords()
        {
            var parser = new IisLogParser();
            var entities = parser.Parse(() => new MemoryStream(File.ReadAllBytes(IisFileName)), new Uri("http://shipish/e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log", UriKind.Absolute), new DiagnosticsSourceSummary()).ToArray();
            Assert.Equal("5RD00155D4A0E2E", entities[0]["s-computername"]);
            Assert.Equal("GET", entities[0]["cs-method"]);
            Assert.Equal("store=AU", entities[0]["cs-uri-query"]);
            Assert.Equal("/product/catalogue/v2/productgroups/ctl/4650127", entities[1]["cs-uri-stem"]);
            Assert.Equal("2016-09-16T05:59:59", entities[0]["@timestamp"]);
            Assert.Equal(GetRowKey(5), entities[0]["RowKey"]);
            Assert.Equal(GetRowKey(29), entities[20]["RowKey"]);
            Assert.Equal(GetRowKey(30), entities[21]["RowKey"]);
            Assert.Equal(114, entities.Length);
        }

        [Fact]
        public void TestDataFile_ExtractsAllRecordsInOffsetRange()
        {
            var parser = new IisLogParser();
            var entities = parser.Parse(
                () => new MemoryStream(File.ReadAllBytes(IisFileName)),
                new Uri("http://shipish/e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log", UriKind.Absolute),
                new DiagnosticsSourceSummary(),
                new ParseCursor(RowOffsets[6]) { EndPosition = RowOffsets[7] }).ToArray();

            Assert.Equal("2016-09-16T05:59:59", entities[0]["@timestamp"]);
            Assert.Equal("6RD00155D4A0E2E", entities[0]["s-computername"]);
            Assert.Equal("GET", entities[0]["cs-method"]);
            Assert.Equal("store=US", entities[0]["cs-uri-query"]);
            Assert.Equal(1, entities.Length);
            Assert.Equal(GetRowKey(6), entities[0]["RowKey"]);
        }

        [Fact]
        public void TestDataFile_ExtractsAllRecordsInBrokenOffsetRange()
        {
            var parser = new IisLogParser();
            var entities = parser.Parse(
                () => new MemoryStream(File.ReadAllBytes(IisFileName)),
                new Uri("http://shipish/e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log", UriKind.Absolute),
                new DiagnosticsSourceSummary(),
                new ParseCursor(RowOffsets[5] + 1) { EndPosition = RowOffsets[7]}).ToArray();

            Assert.Equal("2016-09-16T05:59:59", entities[0]["@timestamp"]);
            Assert.Equal("6RD00155D4A0E2E", entities[0]["s-computername"]);
            Assert.Equal("GET", entities[0]["cs-method"]);
            Assert.Equal("store=US", entities[0]["cs-uri-query"]);
            Assert.Equal(1, entities.Length);
            Assert.Equal(GetRowKey(6), entities[0]["RowKey"]);
        }

        [Fact]
        public void TestDataFile_ExtractsAllRecordsStartingFromOffsetWithChangingHeaders()
        {
            var parser = new IisLogParser();
            var entities = parser.Parse(
                () => new MemoryStream(File.ReadAllBytes(IisFileName)),
                new Uri("http://shipish/e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log", UriKind.Absolute),
                new DiagnosticsSourceSummary(),
                new ParseCursor(RowOffsets[30])).ToArray();

            Assert.Equal("2016-09-16T05:00:00", entities[0]["@timestamp"]);
            Assert.Equal("2016-09-16T05:00:01", entities.Last()["@timestamp"]);
            Assert.Equal("30RD00155D4A0E2E", entities[0]["s-computername"]);
            Assert.Equal("GET", entities[0]["cs-method"]);
            Assert.Equal("store=US", entities[0]["cs-uri-query"]);
            Assert.Equal("/product/catalogue/v2/productgroups/ctl/6385565", entities[0]["cs-uri-stem"]);
            Assert.Equal("94", entities[0]["time-taken"]);
            
            Assert.Equal(93, entities.Length);
            Assert.Equal(GetRowKey(30), entities[0]["RowKey"]);
            Assert.Equal(GetRowKey(31), entities[1]["RowKey"]);
        }
    }
}