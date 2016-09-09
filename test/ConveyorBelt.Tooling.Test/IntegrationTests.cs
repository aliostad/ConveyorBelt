using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Scheduling;
using Moq;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class IntegrationTests
    {
        private string _connectionString = null;
        private string _elasticsearchHost = null;

        public IntegrationTests()
        {
            _connectionString = Environment.GetEnvironmentVariable("MasterSchedulerReads_cn");
            if(string.IsNullOrWhiteSpace(_connectionString))
                throw new Exception("PLEASE SETUP ENVVAR!!! MasterSchedulerReads_cn");
            _elasticsearchHost = Environment.GetEnvironmentVariable("MasterSchedulerReads_es");
            if (string.IsNullOrWhiteSpace(_connectionString))
                throw new Exception("PLEASE SETUP ENVVAR!!! MasterSchedulerReads_es");
        }

    }
}
