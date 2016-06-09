using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Parsing;
using Xunit;
using Xunit.Extensions;

namespace ConveyorBelt.Tooling.Test.Parsing
{
    public class SitecoreLogLineParserShould
    {
        private readonly DateTime _logFileDate = new DateTime(2016, 6, 6);

        private readonly DateTime[] _logDates =
        {
            new DateTime(2016, 6, 6, 17, 12, 32),
            new DateTime(2016, 6, 6, 11, 10, 2),
            new DateTime(2016, 6, 6, 19, 31, 16)
        };

        private SitecoreLogLineParser _parser;
        protected SitecoreLogLineParser Parser { get { return _parser ?? (_parser = new SitecoreLogLineParser()); } }

        [Theory]
        [InlineData(
            "ManagedPoolThread #0 17:12:32 INFO  Trying to load XML configuration /App_Config/Security/GlobalRoles.config",
            "INFO", "ManagedPoolThread #0", "Trying to load XML configuration /App_Config/Security/GlobalRoles.config",
            0)]
        [InlineData("17436 11:10:02 DEBUG Test Debug message", "DEBUG", "17436", "Test Debug message", 1)]
        [InlineData("17436 19:31:16 ERROR Test Error message", "ERROR", "17436", "Test Error message", 2)]
        public void ReturnInfoLog(string log, string level, string eventSource, string text, int logDateIndex)
        {
            var result = Parser.ParseLine(log, _logFileDate);
            Assert.NotNull(result);
            Assert.Equal(result.Level, level);
            Assert.Equal(result.EventSource, eventSource);
            Assert.Equal(result.Message, text);
            Assert.Equal(result.LogDateTime, _logDates[logDateIndex]);
        }

        [Fact]
        public void ReturnNullOnPartialLog()
        {
            
            var result =
                Parser.ParseLine(
                    "   at System.Reflection.RuntimeConstructorInfo.Invoke(BindingFlags invokeAttr, Binder binder, Object[] parameters, CultureInfo culture)",
                    _logFileDate);
            Assert.Null(result);
        }
    }
}