using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive.DataStructures;
using Microsoft.WindowsAzure.Storage.Table;
using Moq;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{
    public class IisLogTests
    {
        [Fact]
        public void TestDataFile_ExtractsAllRecords()
        {
            var stream = new MemoryStream(File.ReadAllBytes("IisLog.txt"));
            var mock = new Mock<IBlob>();
            mock.SetupGet(x => x.Body).Returns(stream);
            mock.SetupGet(x => x.Id)
                .Returns(
                    "e277461e28dd4309af674f083094c568/Test.Presentation.Web.Api/Test.Presentation.Web.Api_IN_0/Web/W3SVC1273337584/u_ex15020701.log");
            var entities = mock.Object.FromIisLogsToEntities().ToArray();
            Assert.NotEmpty(entities);

            var oneBeforeLast = entities.Reverse().Skip(1).First();

            Assert.Equal(12419L, oneBeforeLast.Properties["time-taken"].Int64Value);
            Assert.Equal("/SlowWebApi/", entities.First().Properties["cs-uri-stem"].StringValue);
            Assert.Equal("e277461e28dd4309af674f083094c568_Test.Presentation.Web.Api_Test.Presentation.Web.Api_IN_0_Web_W3SVC1273337584", entities.First().PartitionKey);
            Assert.Equal("u_ex15020701_20150106185346_aa59aa70-9637-4c9a-8aec-f7e40e6a8ae2", entities.First().RowKey);
        }
    }
}
