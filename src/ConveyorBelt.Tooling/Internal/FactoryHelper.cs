using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using ConveyorBelt.Tooling.Parsing;

namespace ConveyorBelt.Tooling.Internal
{
    public static class FactoryHelper
    {
        public static T Create<T>(string typeNameString, Type defaultConcreteType = null)
        {
            if (!string.IsNullOrEmpty(typeNameString))
            {
                defaultConcreteType = Assembly.GetExecutingAssembly().GetType(typeNameString) ?? Type.GetType(typeNameString);
                if (defaultConcreteType == null)
                    throw new InvalidOperationException("Type was not found: " + typeNameString);
            }

            if (defaultConcreteType == null)
            {
                throw new ArgumentException("Type was not specified and defaultConcreteType was null.");                
            }

            return (T)Activator.CreateInstance(defaultConcreteType);
        }
    }
}
