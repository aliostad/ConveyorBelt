using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using ConveyorBelt.Tooling.Internal;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Parsing
{
    public class SitecoreLogParser : IParser
    {
        public static class SitecoreLogFields
        {
            public static readonly string Level = "SitecoreLevel";
            public static readonly string Message = "Payload";
            public static readonly string ProcessId = "SitecoreProcessId";
        }

        public IEnumerable<DynamicTableEntity> Parse(Stream body, Uri id, long position = 0, long endPosition = 0)
        {
            if (body.Position != 0)
                body.Position = 0;

            if (endPosition == 0)
                endPosition = long.MaxValue;
            
            string line;
            var lineNumber = 0;
            var fileName = GetFileName(id);

            var idSegments = id.Segments.Skip(2).Select(x => x.Replace("/", "")).ToArray();
            var partionKey = string.Join("_", idSegments.Take(idSegments.Length - 1));
            var fileDate = GetFileDate(fileName);

            var logLineParser = new SitecoreLogLineParser();

            SitecoreLogEntry currentEntry = null;
            var reader = new StreamReader(body);
            while (body.Position < endPosition && (line = reader.ReadLine()) != null)
            {
                lineNumber++;

                if (body.Position < position)
                    continue;

                if (string.IsNullOrWhiteSpace(line))
                    continue;

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
                    yield return MapLogItemToTableEntity(currentEntry, partionKey, fileName);
                    currentEntry = logItem;
                }
            }

            if (currentEntry != null)
                yield return MapLogItemToTableEntity(currentEntry, partionKey, fileName); 
        }

        private static DynamicTableEntity MapLogItemToTableEntity(SitecoreLogEntry logItem, string partitionKey,
            string fileName)
        {
            var entity = new DynamicTableEntity
            {
                Timestamp = new DateTimeOffset(logItem.LogDateTime),
                PartitionKey = partitionKey,
                RowKey = string.Format("{0}_{1}", fileName, logItem.LogLineNumber)
            };
            entity.Properties.Add(SitecoreLogFields.Level, new EntityProperty(logItem.Level));
            entity.Properties.Add(SitecoreLogFields.ProcessId, new EntityProperty(logItem.EventSource));
            entity.Properties.Add(SitecoreLogFields.Message, new EntityProperty(logItem.Message));
            return entity;
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
                if (DateTime.TryParseExact(datePart, "yyyyddMM", new DateTimeFormatInfo(),
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