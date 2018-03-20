using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ConveyorBelt.Tooling.Configuration;

namespace ConveyorBelt.Tooling.Parsing
{
    public class IisLogParser : IParser
    {
        public IEnumerable<IDictionary<string, string>> Parse(Stream body, Uri id, DiagnosticsSourceSummary source, long startPosition = 0, long endPosition = 0)
        {
            return ParseInternal(body, id, source, startPosition, startPosition, endPosition);
        }

        private IEnumerable<IDictionary<string, string>> ParseInternal(Stream body, Uri id, DiagnosticsSourceSummary source, long startReadPosition, long startParsePosition, long endPosition)
        {
            if (endPosition == 0)
                endPosition = long.MaxValue;

            var idSegments = id.Segments.Skip(2).Select(x => x.Replace("/", "")).ToArray();
            var partitionKey = string.Join("_", idSegments.Take(idSegments.Length - 1));
            var rowKeyPrefix = Path.GetFileNameWithoutExtension(idSegments.Last());

            var fields = startReadPosition > 0 ? ReadFirstHeaders(body) : null;
            body.Seek(startReadPosition, SeekOrigin.Begin);

            var headersHaveChanged = false;
            using (var reader = new StreamReader(body, Encoding.ASCII, true, 1024 * 1024 * 5, true))
            {
                string line;
                var offset = startReadPosition;
                var lineCount = 0;
                while (offset < endPosition && (line = reader.ReadLine()) != null)
                {
                    lineCount++;
                    offset += line.Length + Environment.NewLine.Length;
                    if (line.StartsWith("#Fields: "))
                    {
                        fields = BuildFields(line);
                        continue;
                    }

                    if (line.StartsWith("#"))
                        continue;

                    //skip until start offset in case of re-read
                    if (offset < startParsePosition)
                        continue;

                    var entries = GetEntries(line);
                    if (fields?.Length + 2 != entries.Length)
                    {
                        if (startReadPosition == 0)
                            throw new InvalidOperationException("fields column mismatch");

                        //e.g. in case startReadPosition was pointing to the middle of the line
                        //have to skip to next line
                        if (lineCount == 1)
                            continue;

                        headersHaveChanged = true;
                        break;
                    }

                    yield return ParseEntity(fields, entries, source.TypeName, partitionKey, $"{rowKeyPrefix}_{offset}");
                }
            }

            if (!headersHaveChanged) yield break;

            //headers have changed since the beginning of the file - have to read whole file
            foreach (var doc in ParseInternal(body, id, source, 0, startParsePosition, endPosition))
            {
                yield return doc;
            }
        }

        private static Dictionary<string, string> ParseEntity(string[] fields, string[] entries, string cbType, string partitionKey, string rowKey)
        {
            const int stampEntryCount = 2;
            var datetime = string.Join("T", entries.Take(stampEntryCount));

            var doc = new Dictionary<string, string>(fields.Length + 3)
            {
                {"@timestamp", datetime},
                {"PartitionKey", partitionKey},
                {"RowKey", rowKey},
                {"cb_type", cbType}
            };

            for (var i = 0; i < fields.Length; i++)
            {
                var name = fields[i];
                var value = entries[stampEntryCount + i];

                if (value == "-") // to get around the missing fields in IIS... this is how it does its
                    continue;

                if (name != DiagnosticsSource.CustomAttributesFieldName)
                {
                    doc[name] = value;
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(value))
                        continue;

                    foreach (var pair in value.Split(';'))
                    {
                        var parts = pair.Split(new[] {'='}, 2);
                        if (parts.Length == 2)
                            doc[parts[0]] = parts[1];
                    }
                }
            }

            return doc;
        }

        private string[] ReadFirstHeaders(Stream body)
        {
            // this is just to make sure we read the fields. If we start in the middle, we will miss the fields
            body.Seek(0, SeekOrigin.Begin);
            using (var headerReader = new StreamReader(body, Encoding.ASCII, true, 1024 * 5, true))
            {
                string line;
                while ((line = headerReader.ReadLine()) != null)
                {
                    if (line.StartsWith("#Fields: "))
                    {
                        return BuildFields(line);
                    }
                }
            }

            return null;
        }

        protected virtual string[] GetEntries(string line)
        {
            return line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
        }

        private string[] BuildFields(string line)
        {
            if (!line.StartsWith("#Fields: date time "))
                throw new InvalidDataException("Does not contain date time as the first fields.");

            line = line.Replace("#Fields: date time ", "")
                       .Replace(")", "")
                       .Replace("(", "_");

            return line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
        }
    }
}
