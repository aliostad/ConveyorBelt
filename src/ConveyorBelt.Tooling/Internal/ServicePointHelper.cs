using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling.Internal
{
    public static class ServicePointHelper
    {
        /// <summary>
        /// 1. Improving performance of HTTP
        /// 2. Making lifetime of a DNS connection shorter
        /// </summary>
        public static void ApplyStandardSettings(string url)
        {
            var sp = ServicePointManager.FindServicePoint(new Uri(url));
            sp.Expect100Continue = false;
            sp.UseNagleAlgorithm = false;
            sp.ConnectionLeaseTimeout = 60*1000; // 1 minute
        }
    }
}
