using ConveyorBelt.Tooling.Parsing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ConveyorBelt.Tooling.Test.Parsing
{
    public class GenericJsonParserTests
    {
        private const string SingleFileName = @"data\single.json";
        private const string AnotherSingleFileName = @"data\anotherSingle.json";
        private const string ArrayFileName = @"data\array.json";

        [Fact]
        public void ParsesSingleFine()
        {
            var stream = new MemoryStream(File.ReadAllBytes(SingleFileName));
            var parser = new GenericJsonParser();
            var dic = parser.Parse(() => stream, null, new DiagnosticsSourceSummary());
            Assert.Equal("is string", dic.First()["this"]);
            Assert.Equal("1234", dic.First()["thisIsNumber"]);
            Assert.Equal("2018-06-02T13:51:05.4810613+01:00", dic.First()["thisIsDate"]);
            Assert.Equal("2018-06-02T13:51:05.4810613+01:00", dic.First()["@timestamp"]);
        }

        [Fact]
        public void ParsesSingleFineMoreDates()
        {
            var stream = new MemoryStream(File.ReadAllBytes(AnotherSingleFileName));
            var parser = new GenericJsonParser();
            var dic = parser.Parse(() => stream, null, new DiagnosticsSourceSummary());
            Assert.Equal("2018-06-02T14:51:05.4810613+01:00", dic.First()["@timestamp"]);
        }

        [Fact]
        public void ParsesArrayFine()
        {
            var stream = new MemoryStream(File.ReadAllBytes(ArrayFileName));
            var parser = new GenericJsonParser();
            var dic = parser.Parse(() => stream, null, new DiagnosticsSourceSummary());
            Assert.Equal("2018-06-02T14:51:05.4810613+01:00", dic.Skip(1).First()["@timestamp"]);
        }
    }
}
