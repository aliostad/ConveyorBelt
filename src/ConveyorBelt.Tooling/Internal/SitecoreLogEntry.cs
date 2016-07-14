using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Internal
{
    public class SitecoreLogEntry
    {
        private readonly StringBuilder _text = new StringBuilder();
        public int LogLineNumber { get; set; }
        public string Level { get; set; }
        public DateTime LogDateTime { get; set; }
        public string EventSource { get; set; }
        public string Message { get { return _text.ToString(); } }

        public void AppendMessageText(string line)
        {
            if (_text.Length == 0)
            {
                _text.Append(line);
            }
            else
            {
                _text.AppendLine();
                _text.Append(line);
            }
        }
    }
}
