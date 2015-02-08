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


        public async Task<IEnumerable<Event>> ScheduleAsync(DiagnosticsSource source)
        {
            var lockToken = new LockToken(source.ToTypeKey());
            if (!(await _lockStore.TryLockAsync(lockToken)))
            {
                TheTrace.TraceInformation("I could NOT be master for {0}", source.ToTypeKey());
                return Enumerable.Empty<Event>();
            }

            return await DoSchedule(source);
        }
    }
}
