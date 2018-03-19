using System;
using System.Collections.Generic;
using System.IO;

namespace ConveyorBelt.Tooling.Parsing
{
    public interface IParser
    {
        IEnumerable<IDictionary<string, string>> Parse(Stream body, Uri id, DiagnosticsSourceSummary source, long startPosition = 0, long endPosition = 0);
    }
}
