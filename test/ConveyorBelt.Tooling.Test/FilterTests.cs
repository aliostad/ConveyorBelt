using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Internal;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;
using Xunit.Extensions;
using Xunit.Sdk;

namespace ConveyorBelt.Tooling.Test
{
    public class FilterTests
    {
        private const string PropertyName = "Shimra";

        [Theory]
        [InlineData("a>b")]
        [InlineData("a >=b")]
        [InlineData("a > b >= ")]
        public void InvalidCases(string filter)
        {
            Assert.False(new SimpleFilter(filter).HasValidExpression);
        }

        [Theory]
        [InlineData("Shimra == true", true, true)]
        [InlineData("Shimra == false", true, false)]
        [InlineData("Shimra == false", false, true)]
        [InlineData("Shimra >= 10", 10, true)]
        [InlineData("Shimra <= 10", 89080, false)]
        [InlineData("Shimra != 4234.4", 4234.4, false)]
        [InlineData("Shimra != shomi", "shomi", false)]
        public void ValidCases(string filter, object value, bool result)
        {
            var entity = new DynamicTableEntity("", "");
            entity.Properties.Add(PropertyName, EntityProperty.CreateEntityPropertyFromObject(value));
            var simpleFilter = new SimpleFilter(filter);
            Assert.True(simpleFilter.HasValidExpression);
            Assert.Equal(result, simpleFilter.Satisfies(entity));
        }

    }
}
