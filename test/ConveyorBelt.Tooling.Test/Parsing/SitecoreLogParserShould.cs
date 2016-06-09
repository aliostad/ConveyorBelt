using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Parsing;
using Xunit;

namespace ConveyorBelt.Tooling.Test.Parsing
{
    
    public class SitecoreLogParserShould
    {
        [Fact]
        public void ParseSingleInfoLevel()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreLog1.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/SitecoreLog1.log.20160606.172129.txt");

                var result = sitecoreLogParser.Parse(stream, uri);
                Assert.NotNull(result);
                var parsedLog = result.FirstOrDefault();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.ProcessId].StringValue, "ManagedPoolThread #0");
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Level].StringValue, "INFO");
                Assert.Equal(parsedLog.Timestamp, DateTimeOffset.Parse("2016-06-06 17:12:32"));
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Message].StringValue, "Trying to load XML configuration /App_Config/Security/GlobalRoles.config");
            }
        }

        [Fact]
        public void ParseSingleDebugLevel()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreLog2.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/asosbaselogfile.20160606.180755.txt");

                var result = sitecoreLogParser.Parse(stream, uri);
                Assert.NotNull(result);
                var parsedLog = result.FirstOrDefault();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Level].StringValue, "DEBUG");
            }
        }

        [Fact]
        public void ParseCompleteErrorLevel()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreLog3.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/sitecoredev228CA/xyz/asosbaselogfile.20160606.180755.txt");

                var result = sitecoreLogParser.Parse(stream, uri).ToList();
                Assert.NotNull(result);
                var parsedLog = result.FirstOrDefault();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Level].StringValue, "ERROR");
                Assert.True(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Message].StringValue.EndsWith("Parameter name: database\r\n"));
                Assert.True(result.Count() == 2);
            }
        }

        [Fact]
        public void ParseMultipleLogs()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreDetailedLog.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/SitecoreLog1.log.20160606.172133.txt");

                var result = sitecoreLogParser.Parse(stream, uri).ToList();
                Assert.NotNull(result);
                Assert.Equal(result.Count, 57);
                var parsedLog = result.Last();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Level].StringValue, "WARN");
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Message].StringValue, "Shutdown message: HostingEnvironment initiated shutdown");

                foreach (var log in result)
                {
                    Assert.NotNull(log);
                    Assert.NotEqual(log.Properties[SitecoreLogParser.SitecoreLogFields.Message].StringValue, string.Empty);
                    Assert.False(log.Properties[SitecoreLogParser.SitecoreLogFields.Message].StringValue.Contains("****"));
                }
            }
        }

        [Fact]
        public void RemoveEmptyLogEntries()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreLog4.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/SitecoreLog1.log.20160606.172133.txt");

                var result = sitecoreLogParser.Parse(stream, uri).ToList();
                Assert.NotNull(result);
                Assert.Equal(result.Count, 1);
                var parsedLog = result.Last();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Level].StringValue, "INFO");
                Assert.Equal(parsedLog.Properties[SitecoreLogParser.SitecoreLogFields.Message].StringValue, "Sitecore started");
            }
        }
    }
}
