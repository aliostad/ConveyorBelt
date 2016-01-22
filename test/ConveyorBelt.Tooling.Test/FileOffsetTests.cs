using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace ConveyorBelt.Tooling.Test
{
    public class FileOffsetTests
    {
        [Theory]
        [InlineData("", false)]
        [InlineData("2016-01-19T21:50:00.0000000+00:00", true)]
        [InlineData("2016-01-19T21:50:00.0000000+00:00	adasa sddssdfsd", true)]
        [InlineData("2016-01-19T21:50:00.0000000+00:00	adasa sddssdfsd	1", true)]
        [InlineData("2016-01-19T21:50:00.0000000+00:00	adasa sddssdfsd	hghg", false)]
        [InlineData("2016-01-19T21:50:	adasa sddssdfsd	1", false)]
        public void Parse(string offset, bool result)
        {
            FileOffset offs = null;
            Assert.Equal(result, FileOffset.TryParse(offset, out offs));
        }
    }
}
