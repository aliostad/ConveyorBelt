using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Events
{
    public class BlobFileScheduled
    {
        public string FileToConsume { get; set; }

        public string PreviousFile { get; set; }

        public string NextFile { get; set; }

        public DiagnosticsSourceSummary Source { get; set; }

        public long LastPosition { get; set; }

        public DateTimeOffset StopChasingAfter { get; set; }

        public bool? IsRepeat { get; set; } // for tracing purposes
    }
}
