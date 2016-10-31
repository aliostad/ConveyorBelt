using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using BeeHive.Configuration;
using BeeHive.DataStructures;
using ConveyorBelt.Tooling.Configuration;
using ConveyorBelt.Tooling.Internal;
using ConveyorBelt.Tooling.Scheduling;
using Microsoft.WindowsAzure.Storage.Table;
using Moq;
using Xunit;

namespace ConveyorBelt.Tooling.Test
{

    public class MinuteTableShardSchedulerTests
    {

        [Fact]
        public void BetweenAnHourWithGraceOf3ThereIs57()
        {
            var lockStore = new Mock<ILockStore>();
            lockStore.Setup(
                x => x.TryLockAsync(It.IsAny<LockToken>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<int>())).Returns(Task.FromResult(true));
            var config = new Mock<IConfigurationValueProvider>();
            var scheduler = new MinuteTableShardScheduler( config.Object);
            var entity = new DynamicTableEntity("dd","fff");
            entity.Properties.Add("GracePeriodMinutes", EntityProperty.GeneratePropertyForInt(3));
            entity.Properties.Add("IsActive", EntityProperty.GeneratePropertyForBool(true));
            var source = new DiagnosticsSource(entity);
            source.LastOffsetPoint = DateTimeOffset.UtcNow.AddHours(-1).DropSecondAndMilliseconds().ToString("O");
            source.LastScheduled = DateTimeOffset.UtcNow.AddDays(-1);

            var result = scheduler.TryScheduleAsync(source).Result;
            Assert.Equal(57, result.Item1.Count());
        }
    }
}
