#pragma once

#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
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

struct zAggregateFunctionRetentionData2
{
    static constexpr auto max_events = 64;

    using Events = std::bitset<max_events>;

    Events events1;
    Events events2;

    void set_event1(UInt8 event)
    {
        events1.set(event);
    }

    void set_event2(UInt8 event)
    {
        events2.set(event);
    }

    void merge(const zAggregateFunctionRetentionData2 & other)
    {
        events1 |= other.events1;
        events2 |= other.events2;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(events1.to_ullong(), buf);
        writeBinary(events2.to_ullong(), buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        UInt64 event_value;

        readBinary(event_value, buf);
        events1 = event_value;

        readBinary(event_value, buf);
        events2 = event_value;
    }
};

class zAggregateFunctionRetention2 final
        : public IAggregateFunctionDataHelper<zAggregateFunctionRetentionData2, zAggregateFunctionRetention2>
{
private:
    const UInt8 eventdays;
    const UInt8 retentiondays;
    const UInt8 datadays;

public:
    String getName() const override
    {
        return "retention2";
    }

    zAggregateFunctionRetention2(
        const DataTypes & arguments, const Array & params, UInt8 eventdays_, UInt8 retentiondays_, UInt8 datadays_)
        : IAggregateFunctionDataHelper<zAggregateFunctionRetentionData2, zAggregateFunctionRetention2>(arguments, params)
        , eventdays(eventdays_)
        , retentiondays(retentiondays_+1)
        , datadays(datadays_)
    {
        for (const auto i : collections::range(0, arguments.size()))
        {
            auto cond_arg = arguments[i].get();
            if (!isUInt8(cond_arg))
                throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i) + " of aggregate function "
                        + getName() + ", must be UInt8",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        /*
        LOG_DEBUG(
            &Poco::Logger::get("zAggregateFunctionRetention"),
            "AggregateFunctionRetention eventdays={},retentiondays={},days={},arguments.size()={}",
            static_cast<Int32>(eventdays),
            static_cast<Int32>(retentiondays),
            static_cast<Int32>(datadays),
            arguments.size());
            */
            
    }


    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>()));
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        /*
        LOG_DEBUG(
            &Poco::Logger::get("zAggregateFunctionRetention"),
            "row_num=={}",row_num);
            */

        auto day = assert_cast<const ColumnVector<UInt8> *>(columns[0])->getData()[row_num];
        auto event1 = assert_cast<const ColumnVector<UInt8> *>(columns[1])->getData()[row_num];
        auto event2 = assert_cast<const ColumnVector<UInt8> *>(columns[2])->getData()[row_num];

        /*
        LOG_DEBUG(
            &Poco::Logger::get("zAggregateFunctionRetention"),
            "days={},event1={},event2={},row_num={}",
            static_cast<Int32>(day),
            static_cast<Int32>(event1),
            static_cast<Int32>(event2),row_num);
            */
            
        if (day >= zAggregateFunctionRetentionData2::max_events)
        {
            return;
        }

        if (event1)
        {
            this->data(place).set_event1(day);
        }

        if (event2)
        {
            this->data(place).set_event2(day);
        }

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

    void insertResultInto(AggregateDataPtr __restrict  place, IColumn & to, Arena *) const override
    {
        /*
        LOG_DEBUG(
            &Poco::Logger::get("zAggregateFunctionRetention"),
            "AggregateFunctionRetention insertResultInto 1={},2={}",
            this->data(place).events1.to_string(),
            this->data(place).events2.to_string());
            */
            

        auto & to_map = assert_cast<ColumnMap &>(to);
        auto & offsets = to_map.getNestedColumn().getOffsets();
        auto & key_column = to_map.getNestedData().getColumn(0);
        auto & value_column = to_map.getNestedData().getColumn(1);

        for (const UInt8 i : collections::range(0, eventdays))
        {
            auto array_size = retentiondays < (datadays - i) ? retentiondays : (datadays - i);
            Array values(array_size+1);

            UInt8 pos = 0;
            const bool first_flag = this->data(place).events1.test(i);
            values[pos++] = first_flag;

            for (const UInt8 m : collections::range(i, array_size + i))
            {
                values[pos++] = first_flag && this->data(place).events2.test(m);
            }

            key_column.insert(i);
            value_column.insert(values);
        }
        offsets.push_back(offsets.back() + eventdays);

        /*
        auto & to_map = assert_cast<ColumnMap &>(to);
        auto & offsets = to_map.getNestedColumn().getOffsets();
        auto & key_column = to_map.getNestedData().getColumn(0);
        auto & value_column = to_map.getNestedData().getColumn(1);

        key_column.insert(0);
        Array values(1);
        values[0] = 2;
        value_column.insert(values);

        key_column.insert(88);
        //Array values(1);
        values[0] = 88;
        value_column.insert(values);

        offsets.push_back(offsets.back() + 2);
        */

    }
};

}
