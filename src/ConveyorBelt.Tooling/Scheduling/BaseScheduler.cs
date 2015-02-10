using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.DataStructures;

namespace ConveyorBelt.Tooling.Scheduling
{
    public abstract class BaseScheduler : ISourceScheduler
    {
        private ILockStore _lockStore;

        public BaseScheduler(ILockStore lockStore)
        {
            _lockStore = lockStore;
        }

        protected abstract Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source);


        public async Task<Tuple<IEnumerable<Event>, bool>> TryScheduleAsync(DiagnosticsSource source)
        {
            // if Stop offset has been reached
            if (source.StopOffsetPoint != null && source.LastOffsetPoint != null && source.LastOffsetPoint.CompareTo(source.StopOffsetPoint) >= 0)
                return new Tuple<IEnumerable<Event>, bool>(Enumerable.Empty<Event>(), false);

            var lockToken = new LockToken(source.ToTypeKey());            
            if (!(await _lockStore.TryLockAsync(lockToken)))
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
