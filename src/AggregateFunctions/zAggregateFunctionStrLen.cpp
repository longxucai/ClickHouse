#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/zAggregateFunctionStrLen.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>



namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    AggregateFunctionPtr
    createAggregateFunctionStrLen(
            const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);

        DataTypePtr data_type = argument_types[0];
        if (!isString(data_type))
            throw Exception(
                "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<AggregateFunctionStrLen<AggregateFunctionStrLenData>>(argument_types);
    }
}

void registerAggregateFunctionStrLen(AggregateFunctionFactory & factory)
{
    factory.registerFunction("aggStrLen", createAggregateFunctionStrLen);
}

}
