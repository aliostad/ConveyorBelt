using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling
{
    public class DefaultHttpClient : IHttpClient
    {

        private HttpClient _client = new HttpClient();

        public Task<HttpResponseMessage> PostAsync(string requestUri, HttpContent content)
        {
            return _client.PostAsync(requestUri, content);
        }
    }
}
