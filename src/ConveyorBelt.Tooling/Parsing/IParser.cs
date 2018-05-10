using System;
using System.Collections.Generic;
using System.IO;

namespace ConveyorBelt.Tooling.Parsing
{
    public interface IParser
    {
        IEnumerable<IDictionary<string, string>> Parse(Func<Stream> streamFactory, Uri id, DiagnosticsSourceSummary source, ParseCursor parseCursor = null);
    }
}
