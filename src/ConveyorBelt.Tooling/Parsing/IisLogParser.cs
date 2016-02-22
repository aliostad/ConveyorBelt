using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Parsing
{
    public class IisLogParser : IParser
    {
        public IEnumerable<DynamicTableEntity> Parse(Stream body, Uri id, long position = 0, long endPosition = 0)
        {
            if (body.Position != 0)
                body.Position = 0;

            if (endPosition == 0)
                endPosition = long.MaxValue;

            var reader = new StreamReader(body);
            string line = null;
            string[] fields = null;
            int lineNumber = 1;
            while (body.Position < endPosition && (line = reader.ReadLine()) != null)
            {
                if (line.StartsWith("#Fields: "))
                    fields = BuildFields(line);

                if (line.StartsWith("#"))
                    continue;

                var idSegments = id.Segments.Skip(2).Select(x => x.Replace("/", "")).ToArray();
                var entries = line.Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries);
                var datetime = string.Join(" ", entries.Take(2));
                var rest = entries.Skip(2).ToArray();
                var entity = new DynamicTableEntity();
                entity.Timestamp = DateTimeOffset.Parse(datetime);
                entity.PartitionKey = string.Join("_", idSegments.Take(idSegments.Length - 1));
                entity.RowKey = string.Format("{0}_{1}",
                    Path.GetFileNameWithoutExtension(idSegments.Last()),
                    lineNumber);

                if (fields.Length != rest.Length)
                    throw new InvalidOperationException("fields not equal");

                for (int i = 0; i < fields.Length; i++)
                {
                    string name = fields[i];
                    string value = rest[i];

                    if (Regex.IsMatch(value, @"^[1-9]\d*$")) // numeric
                        entity.Properties.Add(name, new EntityProperty(Convert.ToInt64(value)));
                    else
                        entity.Properties.Add(name, new EntityProperty(value));
                }

                lineNumber++;
                yield return entity;
            }
        }

        private static string[] BuildFields(string line)
        {
            line = line.Replace("#Fields: ", "");
            if (!line.StartsWith("date time "))
                throw new InvalidDataException("Does not contain date time as the first fields.");

            line = line.Replace("date time ", "");
            line = line.Replace(")", "");
            line = line.Replace("(", "_");
            return line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
        }
    }
}
