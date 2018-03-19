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
            if (body.Position != 0) // this is just to make sure we read the fields. If we start in the middle, we will miss the fields
                body.Position = 0;

            if (endPosition == 0)
                endPosition = long.MaxValue;

            var idSegments = id.Segments.Skip(2).Select(x => x.Replace("/", "")).ToArray();
            var partitionKey = string.Join("_", idSegments.Take(idSegments.Length - 1));
            var rowKeyPrefix = Path.GetFileNameWithoutExtension(idSegments.Last());

            string[] fields = null;

            if (startPosition > 0)
            {
                using (var headerReader = new StreamReader(body, Encoding.ASCII, true, 1024 * 5, true))
                {
                    string line;
                    var offset = 0L;

                    while (offset < endPosition && (line = headerReader.ReadLine()) != null)
                    {
                        offset += line.Length;
                        if (line.StartsWith("#Fields: "))
                        {
                            fields = BuildFields(line);
                        }
                    }
                }
            }

            body.Seek(startPosition, SeekOrigin.Begin);

            var firstDocReturned = false;
            using (var reader = new StreamReader(body, Encoding.ASCII, true, 1024 * 1024 * 5, true))
            {
                string line;
                var offset = startPosition;
                while (offset < endPosition && (line = reader.ReadLine()) != null)
                {
                    offset += line.Length;
                    if (line.StartsWith("#Fields: "))
                    {
                        fields = BuildFields(line);
                        continue;
                    }

                    if (line.StartsWith("#"))
                        continue;

                    var entries = GetEntries(line);
                    if (fields?.Length + 2 != entries.Length)
                    {
                        if (startPosition != 0 && firstDocReturned)
                            break;

                        throw new InvalidOperationException("fields column mismatch");
                    }

                    const int stampEntryCount = 2;
                    var datetime = string.Join("T", entries.Take(stampEntryCount));

                    var doc = new Dictionary<string, string>(fields.Length + 3)
                    {
                        {"@timestamp", datetime},
                        {"PartitionKey", partitionKey},
                        {"RowKey", $"{rowKeyPrefix}_{offset}"},
                        {"cb_type", source.TypeName}
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

                    firstDocReturned = true;
                    yield return doc;
                }
            }

            if(firstDocReturned)
                yield break;

            foreach (var doc in Parse(body, id, source, 0, endPosition))
            {
                yield return doc;
            }
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
