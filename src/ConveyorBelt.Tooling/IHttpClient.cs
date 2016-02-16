using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling
{
    public interface IHttpClient
    {
        Task<HttpResponseMessage> PostAsync(string requestUri, HttpContent content);

        Task<HttpResponseMessage> GetAsync(string requestUri);

        Task<HttpResponseMessage> PutAsJsonAsync(string requestUri, string payload);

        Task<HttpResponseMessage> PutAsync(string requestUri, HttpContent content);

    }
}
