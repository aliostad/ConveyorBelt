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
            var lockToken = new LockToken(source.ToTypeKey());
            
            if (!(await _lockStore.TryLockAsync(lockToken)))
            {
                TheTrace.TraceInformation("I could NOT be master for {0}", source.ToTypeKey());
                return new Tuple<IEnumerable<Event>, bool>(Enumerable.Empty<Event>(), false);
            }

            return new Tuple<IEnumerable<Event>, bool>(await DoSchedule(source), true);
        }
    }
}
