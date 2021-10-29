#pragma once

#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <common/range.h>
#include <bitset>
#include <common/logger_useful.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct zAggregateFunctionRetentionData
{
    static constexpr auto max_events = 32;

    using Events = std::bitset<max_events>;

    Events events;

    void add(UInt8 event)
    {
        events.set(event);
    }

    void merge(const zAggregateFunctionRetentionData & other)
    {
        events |= other.events;
    }

    void serialize(WriteBuffer & buf) const
    {
        UInt32 event_value = events.to_ulong();
        writeBinary(event_value, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        UInt32 event_value;
        readBinary(event_value, buf);
        events = event_value;
    }
};

/**
  * The max size of events is 32, that's enough for retention analytics
  *
  * Usage:
  * - retention(cond1, cond2, cond3, ....)
  * - returns [cond1_flag, cond1_flag && cond2_flag, cond1_flag && cond3_flag, ...]
  */
class zAggregateFunctionRetention final
        : public IAggregateFunctionDataHelper<zAggregateFunctionRetentionData, zAggregateFunctionRetention>
{
private:
    UInt8 events_size;

public:
    String getName() const override
    {
        return "zretention";
    }

    zAggregateFunctionRetention(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<zAggregateFunctionRetentionData, zAggregateFunctionRetention>(arguments, {})
    {

        for (const auto i : collections::range(0, arguments.size()))
        {
            auto cond_arg = arguments[i].get();
            if (!isUInt8(cond_arg))
                throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i) + " of aggregate function "
                        + getName() + ", must be UInt8",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        events_size = static_cast<UInt8>(arguments.size());


        LOG_DEBUG(
            &Poco::Logger::get("zAggregateFunctionRetention"),
            "AggregateFunctionRetention zzzzzzzzzzzzzzzzzzzzzz events_size={}",
            static_cast<Int32>(events_size));
    }


    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        std::string s;

        for (const auto i : collections::range(0, events_size))
        {
            auto event = assert_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
            if (event)
            {
                this->data(place).add(i);
            }

            s = s + "," + std::to_string(static_cast<Int32>(event));

        }

                     LOG_DEBUG(
            &Poco::Logger::get("zAggregateFunctionRetention"),
            "AggregateFunctionRetention row_num={},event={}",
            row_num,
            s);

    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        LOG_DEBUG(&Poco::Logger::get("zAggregateFunctionRetention"), "AggregateFunctionRetention insertResultInto zzzzzzzzzzzzzzzzzzzzzz");

        auto & data_to = assert_cast<ColumnUInt8 &>(assert_cast<ColumnArray &>(to).getData()).getData();
        auto & offsets_to = assert_cast<ColumnArray &>(to).getOffsets();

        ColumnArray::Offset current_offset = data_to.size();
        data_to.resize(current_offset + events_size);

        LOG_DEBUG(
            &Poco::Logger::get("zAggregateFunctionRetention"),
            "current_offset={},events_size={}",
            current_offset,
            static_cast<Int32>(events_size));

        const bool first_flag = this->data(place).events.test(0);
        data_to[current_offset] = first_flag;
        ++current_offset;

        for (size_t i = 1; i < events_size; ++i)
        {
            data_to[current_offset] = (first_flag && this->data(place).events.test(i));
            ++current_offset;
        }

                LOG_DEBUG(
            &Poco::Logger::get("zAggregateFunctionRetention"),
            "current_offset={},events_size={}",
            current_offset,
            static_cast<Int32>(events_size));

        offsets_to.push_back(current_offset);
    }
};

}
