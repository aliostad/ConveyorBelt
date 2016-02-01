using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading.Tasks;
using BeeHive;
using Microsoft.WindowsAzure.Storage.Table;

namespace ConveyorBelt.Tooling.Internal
{
    /// <summary>
    /// Class representing a single expression, e.g. "Level > 3"
    /// SPACE IS MANDATORY!!!!!!!
    /// </summary>
    internal class SimpleFilter
    {
        private static Dictionary<string, Func<Expression, Expression, BinaryExpression>> PredicateSymbols = new
        Dictionary<string, Func<Expression, Expression, BinaryExpression>>
        {
            {" > ", Expression.GreaterThan},
            {" < ", Expression.LessThan},
            {" >= ", Expression.GreaterThanOrEqual},
            {" <= ", Expression.LessThanOrEqual},
            {" != ", Expression.NotEqual},
            {" == ", Expression.Equal}
        };

        //private Func<object, bool> _condition;
        private dynamic _condition;
        private string _propertyName;

        public SimpleFilter(string expression)
        {
            _condition = ParseExpression(expression);
        }

        public bool HasValidExpression
        {
            get
            {
                return _condition != null;
            
            }
        }

        public bool Satisfies(DynamicTableEntity entity)
        {
            if (_condition == null)
                return true;

            if (!entity.Properties.ContainsKey(_propertyName))
                return true;

            if(_condition.GetType()==typeof(Func<int, bool>))
                return _condition(entity.Properties[_propertyName].Int32Value.Value);

            if (_condition.GetType() == typeof(Func<bool, bool>))
                return _condition(entity.Properties[_propertyName].BooleanValue.Value);

            if (_condition.GetType() == typeof(Func<float, bool>))
                return _condition((float) entity.Properties[_propertyName].DoubleValue.Value);

            if (_condition.GetType() == typeof(Func<DateTime, bool>))
                return _condition(entity.Properties[_propertyName].DateTimeOffsetValue.Value.DateTime);

            if (_condition.GetType() == typeof(Func<Guid, bool>))
                return _condition(entity.Properties[_propertyName].GuidValue.Value);

            return _condition(entity.Properties[_propertyName].StringValue);

        }

        private object ParseExpression(string expression)
        {
            if (string.IsNullOrWhiteSpace(expression))
                return null;

            var symbols = PredicateSymbols.Keys.Where(expression.Contains).ToArray();

            if (symbols.Length > 1)
            {
                TheTrace.TraceWarning("Expression has multiple predicates: {0}", expression);
                return null;
            }

            if (symbols.Length == 0)
            {
                TheTrace.TraceWarning("Expression has no predicates: {0}", expression);
                return null;
            }

            var splits = expression.Split(new string[]{symbols[0]}, StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Trim()).ToArray();

            if (splits.Length != 2)
            {
                TheTrace.TraceWarning("Expression incomplete: {0}", expression);
                return null;
            }


            string left = splits[0];
            string right = splits[1];
            _propertyName = left;

            int ic = 0;
            if (int.TryParse(right, out ic))
            {
                var p1 = Expression.Parameter(typeof(int), "x");

                return Expression.Lambda<Func<int, bool>>(
                    PredicateSymbols[symbols[0]](p1, Expression.Constant(ic)), 
                    p1).Compile();
            }

            float iff;
            if (float.TryParse(right, out iff))
            {
                var p3 = Expression.Parameter(typeof(float), "x");

                return Expression.Lambda<Func<float, bool>>(
                    PredicateSymbols[symbols[0]](p3, Expression.Constant(iff)),
                    p3).Compile();
            }

            DateTime id;
            if (DateTime.TryParse(right, out id))
            {
                var p2 = Expression.Parameter(typeof(DateTime), "x");

                return Expression.Lambda<Func<DateTime, bool>>(
                    PredicateSymbols[symbols[0]](p2, Expression.Constant(id)),
                    p2).Compile();
            }

            Guid ig;
            if (Guid.TryParse(right, out ig))
            {
                var p4 = Expression.Parameter(typeof(Guid), "x");

                return Expression.Lambda<Func<Guid, bool>>(
                    PredicateSymbols[symbols[0]](p4, Expression.Constant(ig)),
                    p4).Compile();
            }

            bool ib;
            if (bool.TryParse(right, out ib))
            {
                var p5 = Expression.Parameter(typeof(bool), "x");

                return Expression.Lambda<Func<bool, bool>>(
                    PredicateSymbols[symbols[0]](p5, Expression.Constant(ib)),
                    p5).Compile();
            }

            // it is string then
            var parameter = Expression.Parameter(typeof(string), "x");

            return Expression.Lambda<Func<string, bool>>(
                PredicateSymbols[symbols[0]](parameter, Expression.Constant(right)),
                parameter).Compile();
           
        }
    }
}
