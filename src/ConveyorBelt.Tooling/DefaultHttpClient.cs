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

        private HttpClient _client = null;

        public DefaultHttpClient(IEnumerable<KeyValuePair<string, string>> defaultHeaders = null)
        {
            _client = new HttpClient();
            if (defaultHeaders != null)
            {
                foreach (var header in defaultHeaders)
                    _client.DefaultRequestHeaders.Add(header.Key, header.Value);
            }
        }

        public Task<HttpResponseMessage> PostAsync(string requestUri, HttpContent content)
        {
            return _client.PostAsync(requestUri, content);
        }

        public Task<HttpResponseMessage> GetAsync(string uri)
        {
            return _client.GetAsync(uri);
        }

        public Task<HttpResponseMessage> PutAsJsonAsync(string requestUri, string payload)
        {
            return _client.PutAsJsonAsync(requestUri, payload);
        }

        public Task<HttpResponseMessage> PutAsync(string requestUri, HttpContent content)
        {
            return _client.PutAsync(requestUri, content);
        }
    }
}
