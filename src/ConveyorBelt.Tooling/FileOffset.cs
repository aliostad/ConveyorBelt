using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling
{
    public class FileOffset
    {
        private const string OffsetFormat = "{0}\t{1}\t{2}";

        private long _position;
        private DateTimeOffset _timeOffset = DateTimeOffset.MinValue;
        private string _fileName;

        private FileOffset()
        {
            
        }

        public FileOffset(string fileName, DateTimeOffset timeOffset, long position = 0)
        {
            _fileName = fileName;
            _timeOffset = timeOffset;
            _position = position;
        }

        public long Position
        {
            get { return _position; }
        }

        public DateTimeOffset TimeOffset
        {
            get { return _timeOffset; }
        }

        public string FileName
        {
            get { return _fileName; }
        }

        public override string ToString()
        {
            return string.Format(OffsetFormat, _timeOffset.ToString("O"), _fileName ?? "", _position);
        }

        public static bool TryParse(string offset, out FileOffset result)
        {
            result = null;
            if (string.IsNullOrWhiteSpace(offset))
                return false;
            var fileOffset = new FileOffset();
            var segments = offset.Split('\t');
            if (segments.Length > 3)
                return false;

            if (!DateTimeOffset.TryParse(segments[0], out fileOffset._timeOffset))
                return false;

            if(segments.Length > 1)
                fileOffset._fileName = segments[1];

            if (segments.Length > 2)
            {
                if (!long.TryParse(segments[2], out fileOffset._position))
                    return false;
            }

            result = fileOffset;
            return true;
        }

        public static FileOffset Parse(string offset)
        {
            FileOffset result = null;
            return TryParse(offset, out result) ? result : null;
        }
    }
}
