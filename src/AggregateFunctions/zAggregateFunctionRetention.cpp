#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/zAggregateFunctionRetention.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createzAggregateFunctionRetention(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (arguments.size() < 2)
        throw Exception("Not enough event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (arguments.size() > zAggregateFunctionRetentionData::max_events)
        throw Exception("Too many event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<zAggregateFunctionRetention>(arguments);
}

}

void registerzAggregateFunctionRetention(AggregateFunctionFactory & factory)
{
    factory.registerFunction("zretention", createzAggregateFunctionRetention);
}

}
