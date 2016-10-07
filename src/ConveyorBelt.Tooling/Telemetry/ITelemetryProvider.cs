using PerfIt;

namespace ConveyorBelt.Tooling.Telemetry
{
    public interface ITelemetryProvider
    {
        SimpleInstrumentor GetInstrumentor<T>();
        void WriteTelemetry(string instanceName, long timeTakeMilli, string context);
    }
}