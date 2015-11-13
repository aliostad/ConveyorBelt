using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Parsing;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class FactoryHelperTests
    {
        [Fact]
        public void CanCreate_From_FullName()
        {
            var parser = FactoryHelper.Create<IParser>("ConveyorBelt.Tooling.Parsing.IisLogParser");
            Assert.Equal(typeof(IisLogParser), parser.GetType());
        }

        [Fact()]
        public void CanCreate_From_Default()
        {
            var parser = FactoryHelper.Create<IParser>("", typeof(IisLogParser));
            Assert.Equal(typeof(IisLogParser), parser.GetType());
        }

        [Fact]
        public void Create_From_NonExistenceThrowsInvalidOp()
        {
            Assert.Throws<InvalidOperationException>( () => FactoryHelper.Create<IParser>("Bob Dylan", typeof(IisLogParser)));            
        }

        [Fact]
        public void Create_From_NullAndNoDefaultThrowsArgEx()
        {
            Assert.Throws<ArgumentException>(() => FactoryHelper.Create<IParser>(null));
        }
    }
}
