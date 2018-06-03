using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Parsing
{
    public class GenericJsonParser : IParser
    {
        public IEnumerable<IDictionary<string, string>> Parse(Func<Stream> streamFactory, 
            Uri id, 
            DiagnosticsSourceSummary source, 
            ParseCursor parseCursor = null)
        {
            var reader = new StreamReader(streamFactory());
            var s = reader.ReadToEnd();
            var j = s.StartsWith("[") ? (JToken) JArray.Parse(s) : JObject.Parse(s);

            var result = new List<IDictionary<string, string>>();
            if (j.Type == JTokenType.Array)
            {
                foreach(var child in j.Children())
                {
                    if (child.Type == JTokenType.Object)
                        result.Add(ToDic(child, source));
                }
            }
            else
            {
                result.Add(ToDic(j, source));
            }

            return result;
        }

        private static IDictionary<string, string> ToDic(JToken singleJ, DiagnosticsSourceSummary source)
        {
            var dic = new Dictionary<string, string>();
            string goodDateTime = null;
            string okDateTime = null;
            string anyDateTime = null;

            foreach (var child in singleJ.Children())
            {
                if(child.Type == JTokenType.Property)
                {
                    var prop = (JProperty)child;
                    var value = ((JValue)prop.Value).Value;

                    if(value != null)
                    {
                        string dateValue = null;
                        if (value.GetType() == typeof(DateTime))
                        {
                            dic.Add(prop.Name, ((DateTime)value).ToString("O"));
                            dateValue = dic[prop.Name];
                        }
                        else if (value.GetType() == typeof(DateTimeOffset))
                        {
                            dic.Add(prop.Name, ((DateTimeOffset)value).ToString("O"));
                            dateValue = dic[prop.Name];
                        }
                        else
                            dic.Add(prop.Name, value.ToString());

                        if(dateValue!=null)
                        {
                            if (prop.Name.Equals("Timestamp", StringComparison.CurrentCultureIgnoreCase) || prop.Name.Equals("EventDate", StringComparison.CurrentCultureIgnoreCase))
                                goodDateTime = goodDateTime ?? dateValue;
                            if (prop.Name.ToLower().Contains("date") || prop.Name.ToLower().Contains("time"))
                                okDateTime = okDateTime ?? dateValue;
                            anyDateTime = anyDateTime ?? dateValue;
                        }
                    }
                }
            }

            var datetimeValue = goodDateTime ?? okDateTime ?? anyDateTime ?? DateTimeOffset.UtcNow.ToString("O");
            dic.Add("@timestamp", datetimeValue);

            if(!dic.ContainsKey("PartitionKey") || !dic.ContainsKey("RowKey"))
            {
                dic["PartitionKey"] = "nopart_";
                dic["RowKey"] = Guid.NewGuid().ToString("N");
            }

            dic["cb_type"] = source.TypeName;

            return dic;
        }
    }
}
