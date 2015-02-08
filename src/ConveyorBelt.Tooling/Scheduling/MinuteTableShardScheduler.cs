using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.DataStructures;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class MinuteTableShardScheduler : BaseScheduler
    {
        public MinuteTableShardScheduler(ILockStore lockStore) : base(lockStore)
        {
        }

        protected async override Task<IEnumerable<Event>> DoSchedule(DiagnosticsSource source)
        {
            throw new NotImplementedException();
        }

        protected virtual string GetShardKey(DateTimeOffset offset)
        {
            return offset.Ticks.ToString("D19");
        }
    }
}
