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

        DiagnosticsSource RefreshSource(DiagnosticsSource source); // goes to data store to get the latest version
    }
}
