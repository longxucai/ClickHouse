#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/zAggregateFunctionRetention2.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_PARAMETER;
    extern const int PARAMETER_OUT_OF_BOUND;
}

namespace
{

AggregateFunctionPtr createzAggregateFunctionRetention2(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    if (params.size() != 3)
        throw Exception("Function " + name + " requires two parameter: eventdays, retentiondays, datadays", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (params[0].getType() != Field::Types::UInt64 || params[1].getType() != Field::Types::UInt64
        || params[2].getType() != Field::Types::UInt64)
        throw Exception("Invalid type for eventdays or retentiondays or datadays", ErrorCodes::UNSUPPORTED_PARAMETER);

    UInt8 eventdays = applyVisitor(FieldVisitorConvertToNumber<UInt8>(), params[0]);
    UInt8 retentiondays = applyVisitor(FieldVisitorConvertToNumber<UInt8>(), params[1]);
    UInt8 datadays = applyVisitor(FieldVisitorConvertToNumber<UInt8>(), params[2]);

    /*
    LOG_DEBUG(
        &Poco::Logger::get("zAggregateFunctionRetention"),
        "AggregateFunctionRetention name={},params.size={},eventdays={},retentiondays={},datadays={}",
        name,
        params.size(),
        static_cast<Int32>(eventdays),
        static_cast<Int32>(retentiondays),
        static_cast<Int32>(datadays));
        */
      

    auto limit = zAggregateFunctionRetentionData2::max_events;
    if (eventdays > limit || retentiondays > limit || datadays > limit)
        throw Exception("Unsupported eventdays or retentiondays or datadays. Should not be greater than " + std::to_string(limit), ErrorCodes::PARAMETER_OUT_OF_BOUND);

    if (eventdays == 0 || retentiondays == 0 || datadays == 0)
        throw Exception("eventdays or retentiondays or datadays should be positive", ErrorCodes::BAD_ARGUMENTS);

    if (arguments.size() != 3)
        throw Exception(
            "Function " + name + " requires three parameter: day, event1, event2", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<zAggregateFunctionRetention2>(arguments, params, eventdays, retentiondays, datadays);
}
}

void registerzAggregateFunctionRetention2(AggregateFunctionFactory & factory)
{
    factory.registerFunction("retention2", createzAggregateFunctionRetention2);
}

}
