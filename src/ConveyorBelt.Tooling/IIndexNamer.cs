using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling
{
    public interface IIndexNamer
    {
        string BuildName(DateTimeOffset time, string typeName);
    }
}
