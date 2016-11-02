using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Internal;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class ServicePointHelperTest
    {
        [Fact]
        public void CallingApplyStandardDoesNotBlowUpInMyFace()
        {
            ServicePointHelper.ApplyStandardSettings("http://google.com");
        }
    }
}
