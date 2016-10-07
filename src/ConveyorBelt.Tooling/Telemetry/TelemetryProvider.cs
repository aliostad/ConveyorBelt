using ConveyorBelt.Tooling.Scheduling;
using PerfIt;

namespace ConveyorBelt.Tooling.Telemetry
{
    public class TelemetryProvider : ITelemetryProvider
    {
        public SimpleInstrumentor GetInstrumentor<T>()
        {
            return new SimpleInstrumentor(new InstrumentationInfo
            {
                CategoryName = "ConveyorBelt",
                Counters = new[] { CounterTypes.AverageTimeTaken },
                InstanceName = typeof(T).Name
            }, false);
        }

        public void WriteTelemetry(string instanceName, long timeTakeMilli, string context)
        {
            InstrumentationEventSource.Instance.WriteInstrumentationEvent(
                "ConveyorBelt",
                instanceName,
                timeTakeMilli, 
                context);
        }
    }
}