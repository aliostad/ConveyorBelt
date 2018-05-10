namespace ConveyorBelt.Tooling.Parsing
{
    public class ParseCursor
    {
        public ParseCursor(long startPosition)
        {
            StartReadPosition = startPosition;
            StartParsePosition = startPosition;
        }

        public long StartReadPosition { get; set; }
        public long StartParsePosition { get; }

        public long EndPosition { get; set; }
    }
}