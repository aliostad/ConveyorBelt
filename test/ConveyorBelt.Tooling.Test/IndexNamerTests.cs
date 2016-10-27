using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive.Configuration;
using Moq;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class IndexNamerTests
    {
        private readonly DateTimeOffset _date = new DateTimeOffset(1969, 8, 18, 9, 0, 0, TimeSpan.Zero); // last evening at woodstock, Jimi Hendrix playing
        private const string TypeName = "vavavoom";

        [Fact]
        public void NoPrefixAndNotOneForType_should_be_date()
        {
            var cfg = new Mock<IConfigurationValueProvider>();
            cfg.Setup(x => x.GetValue(ConfigurationKeys.EsOneIndexPerType)).Returns(false.ToString());
            cfg.Setup(x => x.GetValue(ConfigurationKeys.EsIndexPrefix)).Returns("");
            var indexNamer = new IndexNamer(cfg.Object);

            Assert.Equal("19690818", indexNamer.BuildName(_date, TypeName));
        }

        [Fact]
        public void WithPrefixAndNotOneForType_should_be_date()
        {
            var cfg = new Mock<IConfigurationValueProvider>();
            cfg.Setup(x => x.GetValue(ConfigurationKeys.EsOneIndexPerType)).Returns(false.ToString());
            cfg.Setup(x => x.GetValue(ConfigurationKeys.EsIndexPrefix)).Returns("PREFIX-");
            var indexNamer = new IndexNamer(cfg.Object);

            Assert.Equal("PREFIX-19690818", indexNamer.BuildName(_date, TypeName));
        }

        [Fact]
        public void WithPrefixAndOneForType_should_be_date()
        {
            var cfg = new Mock<IConfigurationValueProvider>();
            cfg.Setup(x => x.GetValue(ConfigurationKeys.EsOneIndexPerType)).Returns(true.ToString());
            cfg.Setup(x => x.GetValue(ConfigurationKeys.EsIndexPrefix)).Returns("PREFIX-");
            var indexNamer = new IndexNamer(cfg.Object);

            Assert.Equal("PREFIX-vavavoom-19690818", indexNamer.BuildName(_date, TypeName));
        }

    }
}
