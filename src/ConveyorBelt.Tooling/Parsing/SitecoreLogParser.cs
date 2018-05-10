using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using ConveyorBelt.Tooling.Internal;

namespace ConveyorBelt.Tooling.Parsing
{
    public class SitecoreLogParser : IParser
    {
        public static class SitecoreLogFields
        {
            public static readonly string Level = "SitecoreLevel";
            public static readonly string Message = "Payload";
            public static readonly string ProcessId = "SitecoreProcessId";
            
            public static readonly string Timestamp = "@timestamp";
            public static readonly string PartitionKey = "PartitionKey";
            public static readonly string RowKey = "RowKey";
            public static readonly string CbType = "cb_type";
        }

        public IEnumerable<IDictionary<string, string>> Parse(Func<Stream> streamFactory, Uri id, DiagnosticsSourceSummary source, ParseCursor cursor = null)
        {
            cursor = cursor ?? new ParseCursor(0);
            var body = streamFactory();
            if (body.Position != 0)
                body.Position = 0;

            if (cursor.EndPosition == 0)
                cursor.EndPosition = long.MaxValue;

            string line;
            var lineNumber = 0;
            var fileName = GetFileName(id);

            var idSegments = id.Segments.Skip(2).Select(x => x.Replace("/", "")).ToArray();
            var partionKey = string.Join("_", idSegments.Take(idSegments.Length - 1));
            var fileDate = GetFileDate(fileName);

            var logLineParser = new SitecoreLogLineParser();

            SitecoreLogEntry currentEntry = null;
            var reader = new StreamReader(body);
            while (body.Position < cursor.EndPosition && (line = reader.ReadLine()) != null)
            {
                lineNumber++;

                if (body.Position < cursor.StartReadPosition)
                    continue;

                if (string.IsNullOrWhiteSpace(line))
                    continue;

                //remove connection string passwords in exceptions etc (thanks sitecore)
                line = ReplaceSecureInformation(line, "password=", "**PASSWORD**REDACTED**");
                line = ReplaceSecureInformation(line, "user id=", "**USER**REDACTED**");

                var logItem = logLineParser.ParseLine(line, fileDate);
                if (logItem != null)
                {
                    var logText = logItem.Message;
                    //filter out rubbish logs. eg blank INFO during sitecore startup
                    if (string.IsNullOrWhiteSpace(logText) || logText.StartsWith("*****"))
                        continue;

                    logItem.LogLineNumber = lineNumber;
                }

                if (logItem == null && currentEntry != null)
                {
                    // Existing multiline message
                    currentEntry.AppendMessageText(line);
                }
                else if (logItem != null && currentEntry == null)
                {
                    // new entry found
                    currentEntry = logItem;
                }
                else if (currentEntry != null)
                {
                    //new entry found, current one is completed.
                    yield return MapLogItemToTableEntity(currentEntry, source, partionKey, fileName);
                    currentEntry = logItem;
                }
            }

            if (currentEntry != null)
                yield return MapLogItemToTableEntity(currentEntry, source, partionKey, fileName);
        }

        /// <summary>
        /// Replace connection string type user and password,
        /// 
        /// </summary>
        /// <param name="line"></param>
        /// <param name="token">What part of connection string to find eg "Password="</param>
        /// <param name="replacementString">what to replace the secure information with eg "--REMOVED--"</param>
        /// <returns></returns>
        private static string ReplaceSecureInformation(string line, string token, string replacementString)
        {
            var tokenIndex = line.IndexOf(token, StringComparison.InvariantCultureIgnoreCase);
            if (tokenIndex >= 0)
            {
                var endofSecureInformation = line.IndexOf(";", tokenIndex, StringComparison.InvariantCulture);
                if (endofSecureInformation < 0)
                {
                    endofSecureInformation = line.Length;
                }
                line = line.Substring(0, tokenIndex + token.Length) + replacementString +
                       line.Substring(endofSecureInformation, line.Length - endofSecureInformation);
            }

            return line;
        }

        private static IDictionary<string, string> MapLogItemToTableEntity(SitecoreLogEntry logItem, DiagnosticsSourceSummary source, string partitionKey, string fileName)
        {
            return new Dictionary<string, string> {
                {SitecoreLogFields.Timestamp, logItem.LogDateTime.ToString("s")},
                {SitecoreLogFields.PartitionKey, partitionKey},
                {SitecoreLogFields.RowKey, $"{fileName}_{logItem.LogLineNumber}"},
                {SitecoreLogFields.CbType, source.TypeName},
                {SitecoreLogFields.Level, logItem.Level},
                {SitecoreLogFields.ProcessId, logItem.EventSource},
                {SitecoreLogFields.Message, logItem.Message}
            };
        }

        private static DateTime GetFileDate(string fileName)
        {
            var fileNameSegments = fileName.Split(new[] {"."}, StringSplitOptions.RemoveEmptyEntries);
            var fileDate = DateTime.Today;

            if (!fileNameSegments.Any())
            {
                throw new ArgumentException(
                    string.Format(
                        "File ID doesn't appear to be a sitecore log file name: '{0}' should be in format of <name>.log.YYYYMMDD.[hhmmss]",
                        fileName));
            }

            foreach (var datePart in fileNameSegments.Skip(fileNameSegments.Length - 2))
            {
                if (DateTime.TryParseExact(datePart, "yyyyMMdd", new DateTimeFormatInfo(),
                    DateTimeStyles.AssumeUniversal, out fileDate))
                {
                    break;
                }
            }

            return fileDate;
        }

        protected string GetFileName(Uri id)
        {
            var idSegments = id.Segments.Skip(2).Select(x => x.Replace("/", "")).ToArray();
            var fileName = Path.GetFileNameWithoutExtension(idSegments.Last() ?? string.Empty);
            return fileName;
        }
    }
}