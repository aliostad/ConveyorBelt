namespace ConveyorBelt.Tooling.Internal
{
    public static class ParserHelper
    {
        private const int Space = ' ';
        private const int Tab = '\t';
        private const int NewLine = '\n';
        private const int CarriageReturn = '\r';
        
        public static int FindNextWord(string text, int index)
        {
            while (index < text.Length &&
                   (text[index] == Space || text[index] == NewLine || (text[index] == CarriageReturn || text[index] == Tab)))
                ++index;
            return index;
        }

        public static int FindPreviousWord(string text, int index)
        {
            while (index - 1 > 0 && index - 1 < text.Length &&
                   (text[index - 1] == Space || text[index - 1] == NewLine || (text[index - 1] == CarriageReturn || text[index - 1] == Tab)))
                --index;
            return index;
        }

        public static int FindNextSpace(string text, int index)
        {
            var nextWhiteSpace = text.IndexOfAny(new []{' ','\t','\n','\r'}, index);
            if (nextWhiteSpace != -1 && text[nextWhiteSpace] != Space && text[nextWhiteSpace] != Tab)
                nextWhiteSpace = -1;
            return nextWhiteSpace;
        }
    }
}