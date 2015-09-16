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
        private ILockStore _lockStore;
        private IConfigurationValueProvider _configurationValueProvider;

        public BaseScheduler(ILockStore lockStore, IConfigurationValueProvider configurationValueProvider)
        {
            _configurationValueProvider = configurationValueProvider;
            _lockStore = lockStore;
        }

        protected abstract Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source);


        public async Task<Tuple<IEnumerable<Event>, bool>> TryScheduleAsync(DiagnosticsSource source)
        {
            // if Stop offset has been reached
            if (!string.IsNullOrEmpty(source.StopOffsetPoint) && source.LastOffsetPoint != null && source.LastOffsetPoint.CompareTo(source.StopOffsetPoint) >= 0)
                return new Tuple<IEnumerable<Event>, bool>(Enumerable.Empty<Event>(), false);

            var lockToken = new LockToken(source.ToTypeKey());
            int seconds =
                Convert.ToInt32(_configurationValueProvider.GetValue(ConfigurationKeys.ClusterLockDurationSeconds));
            if (!(await _lockStore.TryLockAsync(lockToken, 2, 1000, seconds * 1000)))
            {
                TheTrace.TraceInformation("I could NOT be master for {0}", source.ToTypeKey());
                return new Tuple<IEnumerable<Event>, bool>(Enumerable.Empty<Event>(), false);
            }
            try
            {
                var events = await DoSchedule(source);
                return new Tuple<IEnumerable<Event>, bool>(events, true);
            }
            finally
            {
                Task.Run( () => _lockStore.ReleaseLockAsync(lockToken)).Wait();
            }
        }
    }
}
