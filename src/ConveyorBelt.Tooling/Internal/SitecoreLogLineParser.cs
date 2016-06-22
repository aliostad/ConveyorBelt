using System;

namespace ConveyorBelt.Tooling.Internal
{
    public class SitecoreLogLineParser
    {
        public virtual SitecoreLogEntry ParseLine(string line, DateTime startDateTime)
        {
            if (string.IsNullOrWhiteSpace(line))
                return null;
            line = line.Trim();
            var logEntry = new SitecoreLogEntry();
            
            var spaceInTheSameLine = ParserHelper.FindNextSpace(line, 0);
            if (spaceInTheSameLine == -1)
                return null;
            
            int dateTimeStartIndex;
            var logDateTime = FindLogDateTime(line, startDateTime, ref spaceInTheSameLine, out dateTimeStartIndex);
            if (!logDateTime.HasValue)
                return null;
            logEntry.LogDateTime = logDateTime.Value;
            logEntry.EventSource = line.Substring(0, ParserHelper.FindPreviousWord(line, dateTimeStartIndex));
            logEntry.Level = GetLogLevel(line, ref spaceInTheSameLine);
            
            logEntry.AppendMessageText(GetText(line, ref spaceInTheSameLine));
            logEntry.LogLineNumber = 1;
            return logEntry;
        }

        protected virtual DateTime? FindLogDateTime(string line, DateTime startDateTime, ref int index, out int dateTimeStartIndex)
        {
            var startPoint = index;
            DateTime? dateTime = null;
            dateTimeStartIndex = -1;
            
            while (!dateTime.HasValue && (startPoint = ParserHelper.FindNextSpace(line, startPoint)) != -1)
            {
                startPoint = ParserHelper.FindNextWord(line, startPoint);
                dateTimeStartIndex = startPoint;
                dateTime = GetDateTime(line, ref startPoint, startDateTime);
            } 

            if (dateTime.HasValue)
            {
                index = startPoint;
            }
            else
            {
                dateTimeStartIndex = -1;
            }
            return dateTime;
        }

        protected virtual DateTime? GetDateTime(string line, ref int startPoint, DateTime startDateTime)
        {
            if (startPoint + 7 >= line.Length || !char.IsDigit(line[startPoint]) || (!char.IsDigit(line[startPoint + 1]) || !char.IsDigit(line[startPoint + 3])) || (!char.IsDigit(line[startPoint + 4]) || !char.IsDigit(line[startPoint + 6]) || !char.IsDigit(line[startPoint + 7])))
                return new DateTime?();
            
            var hour = (line[startPoint] - 48) * 10 + line[startPoint + 1] - 48;
            var minute = (line[startPoint + 3] - 48) * 10 + line[startPoint + 4] - 48;
            var second = (line[startPoint + 6] - 48) * 10 + line[startPoint + 7] - 48;
            
            if (!IsTimeValid(hour, minute, second))
                return new DateTime?();
            
            startPoint = ParserHelper.FindNextWord(line, startPoint + 8);
            return hour < startDateTime.Hour ? 
                new DateTime(startDateTime.Year, startDateTime.Month, startDateTime.AddDays(1).Day, hour, minute, second) 
                : new DateTime(startDateTime.Year, startDateTime.Month, startDateTime.Day, hour, minute, second);
        }

        private static bool IsTimeValid(int hour, int minute, int second)
        {
            if (IsBetween(hour, DateTime.MinValue.Hour, DateTime.MaxValue.Hour) && IsBetween(minute, DateTime.MinValue.Minute, DateTime.MaxValue.Minute))
                return IsBetween(second, DateTime.MinValue.Second, DateTime.MaxValue.Second);
            return false;
        }

        private static bool IsBetween(int value, int min, int max)
        {
            if (value >= min)
                return value <= max;
            return false;
        }

        protected virtual string GetLogLevel(string line, ref int index)
        {
            var nextSpaceLocation = line.IndexOf(' ', index);
            string str;
            if (nextSpaceLocation != -1)
            {
                str = line.Substring(index, nextSpaceLocation - index);
                index = ParserHelper.FindNextWord(line, nextSpaceLocation);
            }
            else
            {
                str = line.Substring(index);
                index = line.Length;
            }
            return str;
        }

        protected virtual string GetText(string line, ref int index)
        {
            return index >= line.Length - 1 ? string.Empty : line.Substring(index);
        }
    }
}
