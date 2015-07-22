using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive.Configuration;

namespace ConveyorBelt.Tooling.Configuration
{
    public interface ISourceConfiguration
    {

        IEnumerable<DiagnosticsSource> GetSources();

        void UpdateSource(DiagnosticsSource source);
    }
}
