using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using BeeHive.DataStructures;
using ConveyorBelt.Tooling.Configuration;

namespace ConveyorBelt.Tooling.Scheduling
{
    public abstract class BaseScheduler : ISourceScheduler
    {
        private IConfigurationValueProvider _configurationValueProvider;

        public BaseScheduler(IConfigurationValueProvider configurationValueProvider)
        {
            _configurationValueProvider = configurationValueProvider;
        }

        protected abstract Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source);


        public async Task<Tuple<IEnumerable<Event>, bool>> TryScheduleAsync(DiagnosticsSource source)
        {
            // if Stop offset has been reached
            if (!string.IsNullOrEmpty(source.StopOffsetPoint) && source.LastOffsetPoint != null && source.LastOffsetPoint.CompareTo(source.StopOffsetPoint) >= 0)
                return new Tuple<IEnumerable<Event>, bool>(Enumerable.Empty<Event>(), false);

            var events = await DoSchedule(source);
            return new Tuple<IEnumerable<Event>, bool>(events, true);
        }
    }
}
