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
                var uri = new Uri("http://localhost/data/SitecoreLog1.log.20160613.172129.txt");

                var result = sitecoreLogParser.Parse(() => stream, uri, new DiagnosticsSourceSummary());
                Assert.NotNull(result);
                var parsedLog = result.FirstOrDefault();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.ProcessId], "ManagedPoolThread #0");
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Level], "INFO");
                Assert.Equal(parsedLog["@timestamp"], "2016-06-13T17:12:32");
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Message], "Trying to load XML configuration /App_Config/Security/GlobalRoles.config");
            }
        }

        [Fact]
        public void ParseSingleDebugLevel()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreLog2.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/baselogfile.20160613.180755.txt");

                var result = sitecoreLogParser.Parse(() => stream, uri, new DiagnosticsSourceSummary());
                Assert.NotNull(result);
                var parsedLog = result.FirstOrDefault();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Level], "DEBUG");
            }
        }

        [Fact]
        public void ParseCompleteErrorLevel()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreLog3.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/sitecoredev228CA/xyz/baselogfile.20160101.180755.txt");

                var result = sitecoreLogParser.Parse(() => stream, uri, new DiagnosticsSourceSummary()).ToList();
                Assert.NotNull(result);
                var parsedLog = result.FirstOrDefault();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Level], "ERROR");
                Assert.True(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].StartsWith("Test Error with exception\r\n"));
                Assert.True(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].EndsWith("Parameter name: database"));
                Assert.True(result.Count == 2);
            }
        }

        [Fact(Skip ="Not used anymore")]
        public void ParseMultipleLogs()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreDetailedLog.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/SitecoreLog1.log.20160606.172133.txt");

                var result = sitecoreLogParser.Parse(() => stream, uri, new DiagnosticsSourceSummary()).ToList();
                Assert.NotNull(result);
                Assert.Equal(50, result.Count);
                var parsedLog = result.Last();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Level], "WARN");
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Message], "Shutdown message: HostingEnvironment initiated shutdown");

                foreach (var log in result)
                {
                    Assert.NotNull(log);
                    Assert.NotEqual(log[SitecoreLogParser.SitecoreLogFields.Message], string.Empty);
                    Assert.False(log[SitecoreLogParser.SitecoreLogFields.Message].Contains("****"));
                }
            }
        }

        [Fact]
        public void ParseExceptionMessage()
        {
            using (var stream = new MemoryStream(File.ReadAllBytes(@"data\SitecoreLog5.txt")))
            {
                var sitecoreLogParser = new SitecoreLogParser();
                var uri = new Uri("http://localhost/data/SitecoreLog1.log.20160606.172133.txt");

                var result = sitecoreLogParser.Parse(() => stream, uri, new DiagnosticsSourceSummary()).ToList();
                Assert.NotNull(result);
                Assert.Equal(result.Count, 2);

                var parsedLog = result.First();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Level], "ERROR");
                Assert.True(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].StartsWith("Test Message1:\r\n"));
                Assert.True(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].Contains("The password failed.  Password=**PASSWORD**REDACTED**\r\n"));
                Assert.False(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].Contains("TESTPassword"));


                parsedLog = result.Last();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Level], "ERROR");
                Assert.True(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].StartsWith("SINGLE MSG: Sitecore heartbeat:\r\n"));
                Assert.True(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].Contains(";Password=**PASSWORD**REDACTED**;"));
                Assert.True(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].Contains("User ID=**USER**REDACTED**;"));
                Assert.False(parsedLog[SitecoreLogParser.SitecoreLogFields.Message].Contains("Not!actuallyApa$$word"));

                foreach (var log in result)
                {
                    Assert.NotNull(log);
                    Assert.NotEqual(log[SitecoreLogParser.SitecoreLogFields.Message], string.Empty);
                    Assert.False(log[SitecoreLogParser.SitecoreLogFields.Message].Contains("****"));
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

                var result = sitecoreLogParser.Parse(() => stream, uri, new DiagnosticsSourceSummary()).ToList();
                Assert.NotNull(result);
                Assert.Equal(result.Count, 1);
                var parsedLog = result.Last();
                Assert.NotNull(parsedLog);
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Level], "INFO");
                Assert.Equal(parsedLog[SitecoreLogParser.SitecoreLogFields.Message], "Sitecore started");
            }
        }
    }
}
