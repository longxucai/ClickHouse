#pragma once

#include <type_traits>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <common/logger_useful.h>




namespace DB
{
struct AggregateFunctionStrLenData
{
    UInt64 sum{};

    void ALWAYS_INLINE add(UInt64 value) { sum += value; }

    void merge(const AggregateFunctionStrLenData & rhs) { sum += rhs.sum; }

    void write(WriteBuffer & buf) const { writeBinary(sum, buf); }

    void read(ReadBuffer & buf) { readBinary(sum, buf); }

    UInt64 get() const { return sum; }
};

template <typename Data>
class AggregateFunctionStrLen final : public IAggregateFunctionDataHelper<Data, AggregateFunctionStrLen<Data>>
{
public:
    String getName() const override { return "aggStrLen"; }

    AggregateFunctionStrLen(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionStrLen<Data>>(argument_types_, {})
    {
        LOG_DEBUG(&Poco::Logger::get("AggregateFunctionStrLen"), "AggregateFunctionStrLen zzzzzzzzzzzzzzzzzzzzzz");
    }

     bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {

        LOG_DEBUG(&Poco::Logger::get("AggregateFunctionStrLen"), "AggregateFunctionStrLen zzzzzzzzzzzzzzzzzzzzzz row_num={}", row_num);

        const auto & column = static_cast<const ColumnString &>(*columns[0]);
        const auto & offsets = column.getOffsets();
        this->data(place).add(offsets[row_num] - offsets[row_num - 1] - 1);

        const auto & in_vec = column.getChars();

        //auto event = assert_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
        auto str = assert_cast<const ColumnString &>(*columns[0]).getDataAt(row_num).toString();

        


        LOG_DEBUG(
            &Poco::Logger::get("AggregateFunctionStrLen"),
            "AggregateFunctionStrLen zzzzzzzzzzzzzzzzzzzzzz length={},in_vec={},sum={},str={}",
            offsets[row_num] - offsets[row_num - 1] - 1,
            reinterpret_cast<const char *>(&in_vec[offsets[row_num - 1]]),
            this->data(place).get(),
            str);
        

    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override { this->data(place).merge(this->data(rhs)); }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override { this->data(place).write(buf); }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override { this->data(place).read(buf); }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        auto & column = static_cast<ColumnVector<UInt64> &>(to);
        column.getData().push_back(this->data(place).get());
    }
};

}
