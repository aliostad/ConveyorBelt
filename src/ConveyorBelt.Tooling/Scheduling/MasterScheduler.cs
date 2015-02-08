using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Scheduling
{
    public class MasterScheduler
    {
        protected CloudTable _table;
        private IEventQueueOperator _eventQueueOperator;

        public MasterScheduler(IEventQueueOperator eventQueueOperator, string connectionString, string tableName)
        {
            _eventQueueOperator = eventQueueOperator;
            var account = CloudStorageAccount.Parse(connectionString);
            var client = account.CreateCloudTableClient();
            _table = client.GetTableReference(tableName);

        }

        public async Task ScheduleSourcesAsync()
        {
            var sources = _table.ExecuteQuery(new TableQuery<DynamicTableEntity>()).Select(x => new DiagnosticsSource(x));
            foreach (var source in sources)
            {
                if (!source.LastScheduled.HasValue)
                    source.LastScheduled = DateTimeOffset.UtcNow.AddYears(-1);

                if(source.LastScheduled.Value.AddMinutes(source.SchedulingFrequencyMinutes.Value) > DateTimeOffset.UtcNow)
                    continue;

                var schedulerType = Assembly.GetExecutingAssembly().GetType(source.SchedulerType) ??
                                    Type.GetType(source.SchedulerType);
                if (schedulerType == null)
                {
                    source.ErrorMessage = "Could not find SchedulerType: " + source.SchedulerType;

                }
                var scheduler = (ISourceScheduler) Activator.CreateInstance(schedulerType);
                var result = await scheduler.TryScheduleAsync(source);
                if (result.Item2)
                {
                    await _eventQueueOperator.PushBatchAsync(result.Item1);
                }
            }
            
        }
    }
}
