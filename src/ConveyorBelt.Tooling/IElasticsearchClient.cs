using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConveyorBelt.Tooling
{
    public interface IElasticsearchClient
    {
        Task<bool> CreateIndexIfNotExistsAsync(string baseUrl, string indexName, string jsonCommand = "");

        Task<bool> MappingExistsAsync(string baseUrl, string indexName, string typeName);

        Task<bool> UpdateMappingAsync(string baseUrl, string indexName, string typeName, string mapping);

    }
}
