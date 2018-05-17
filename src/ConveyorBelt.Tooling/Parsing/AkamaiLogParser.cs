using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Parsing
{
    public class AkamaiLogParser : IisLogParser
    {
        protected override string[] GetEntries(string line)
        {
            return line.Split('\t').Select(x => x.Trim('"')).ToArray(); // TODO: does not cover the case where there is a TAB inside fields
        }
    }
}
