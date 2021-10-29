#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <memory>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include "array/arrayIndex.h"
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

// map(x, y, ...) is a function that allows you to make key-value pair
class zFunctionMap : public IFunction
{
public:
    static constexpr auto name = "zmap";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<zFunctionMap>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        LOG_DEBUG(&Poco::Logger::get("zFunctionMap"), "zFunctionMap zzzzzzzzzzzzzzzzzzzzzz");

        if (arguments.size() % 2 != 0)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires even number of arguments, but {} given", getName(), arguments.size());

        DataTypes keys, values;
        for (size_t i = 0; i < arguments.size(); i += 2)
        {
            keys.emplace_back(arguments[i]);
            values.emplace_back(arguments[i + 1]);
        }

        

        DataTypes tmp;
        tmp.emplace_back(getLeastSupertype(keys));
        tmp.emplace_back(getLeastSupertype(values));

        LOG_DEBUG(&Poco::Logger::get("zFunctionMap"), "zFunctionMap arguments.size={},keys.size={},tmp.size={}", arguments.size(), keys.size(),tmp.size());

        return std::make_shared<DataTypeMap>(tmp);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        LOG_DEBUG(
            &Poco::Logger::get("zFunctionMap"),
            "zFunctionMap executeImpl arguments.size={},input_rows_count={}",
            arguments.size(), input_rows_count);

        size_t num_elements = arguments.size();

        if (num_elements == 0)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();

        Columns columns_holder(num_elements);
        ColumnRawPtrs column_ptrs(num_elements);

        for (size_t i = 0; i < num_elements; ++i)
        {
            const auto & arg = arguments[i];
            const auto to_type = i % 2 == 0 ? key_type : value_type;

            ColumnPtr preprocessed_column = castColumn(arg, to_type);
            preprocessed_column = preprocessed_column->convertToFullColumnIfConst();

            columns_holder[i] = std::move(preprocessed_column);
            column_ptrs[i] = columns_holder[i].get();
        }

        /// Create and fill the result map.

        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        size_t total_elements = input_rows_count * num_elements / 2;
        keys_data->reserve(total_elements);
        values_data->reserve(total_elements);
        offsets->reserve(input_rows_count);

        LOG_DEBUG(
            &Poco::Logger::get("zFunctionMap"),
            "zFunctionMap executeImpl total_elements={},input_rows_count={}",
            total_elements,
            input_rows_count);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (size_t j = 0; j < num_elements; j += 2)
            {
                keys_data->insertFrom(*column_ptrs[j], i);
                values_data->insertFrom(*column_ptrs[j + 1], i);
            }

            current_offset += num_elements / 2;
            offsets->insert(current_offset);

            LOG_DEBUG(
                &Poco::Logger::get("zFunctionMap"),
                "zFunctionMap executeImpl current_offset={},input_rows_count={}",
                current_offset,
                input_rows_count);
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(nested_column);
    }
};



class zFunctionHelloStr : public IFunction
{
public:
    static constexpr auto name = "zstr";

    static FunctionPtr create(ContextPtr) { return std::make_shared<zFunctionHelloStr>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        LOG_DEBUG(&Poco::Logger::get(" zFunctionHelloStr"), " zFunctionHelloStr zzzzzzzzzzzzzzzzzzzzzz");

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        LOG_DEBUG(&Poco::Logger::get(" zFunctionHelloStr"), " zFunctionHelloStr arguments.size={},input_rows_count={}",arguments.size(), input_rows_count);
        //auto col_res = ColumnVector<UInt64>::create();
        //auto & res_data = col_res->getData();
        //res_data.resize(offsets.size());


        auto col_res = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            String s = "test" + std::to_string(i);
            col_res->insert(s);
        }




        /*
        ColumnString::Chars & res_data = col_res->getChars();
        ColumnString::Offsets & res_offsets = col_res->getOffsets();
        res_offsets.resize(input_rows_count);

        size_t res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            String s = "test" + std::to_string(i);
            res_data.resize(res_data.size() + s.size() + 1);

            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], s.c_str(), s.size());
            res_offset += s.size() + 1;
            res_data[res_offset - 1] = 0;

            res_offsets[i] = res_offset;
        }
        */

        return col_res;
    }
};



class zFunctionHelloInt : public IFunction
{
public:
    static constexpr auto name = "zint";

    static FunctionPtr create(ContextPtr) { return std::make_shared<zFunctionHelloInt>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        LOG_DEBUG(&Poco::Logger::get(" zFunctionHelloInt"), " zFunctionHelloInt zzzzzzzzzzzzzzzzzzzzzz");

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        LOG_DEBUG(
            &Poco::Logger::get(" zFunctionHelloInt"),
            " zFunctionHelloInt arguments.size={},input_rows_count={}",
            arguments.size(),
            input_rows_count);
        //auto col_res = ColumnVector<UInt64>::create();
        //auto & res_data = col_res->getData();
        //res_data.resize(offsets.size());

        auto col_res = ColumnUInt8::create();

        //auto col_res = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            col_res->insert(i);
        }


        return col_res;
    }
};


class zFunctionHelloArray : public IFunction
{
public:
    static constexpr auto name = "zarray";

    static FunctionPtr create(ContextPtr) { return std::make_shared<zFunctionHelloArray>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        LOG_DEBUG(&Poco::Logger::get(" zFunctionHelloArray"), " zFunctionHelloArray zzzzzzzzzzzzzzzzzzzzzz");

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        LOG_DEBUG(
            &Poco::Logger::get(" zFunctionHelloArray"),
            " zFunctionHelloArray arguments.size={},input_rows_count={}",
            arguments.size(),
            input_rows_count);
        //auto col_res = ColumnVector<UInt64>::create();
        //auto & res_data = col_res->getData();
        //res_data.resize(offsets.size());

        auto dst = ColumnArray::create(ColumnUInt64::create());

        

        
        

        auto & dst_data = dst->getData();
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);
        auto current_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (size_t g = 0; g < i; g++)
            {
                ++current_offset;
                dst_data.insert(g);
            }
            dst_offsets[i] = current_offset;
        }

        return dst;
    }
};


class zFunctionHelloMap : public IFunction
{
public:
    static constexpr auto name = "zzmap";

    static FunctionPtr create(ContextPtr) { return std::make_shared<zFunctionHelloMap>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        LOG_DEBUG(&Poco::Logger::get(" zFunctionHelloMap"), " zFunctionHelloMap zzzzzzzzzzzzzzzzzzzzzz");



        return std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>()));
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        LOG_DEBUG(
            &Poco::Logger::get(" zFunctionHelloMap"),
            " zFunctionHelloMap arguments.size={},input_rows_count={}",
            arguments.size(),
            input_rows_count);


        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();

        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        
        keys_data->reserve(input_rows_count);
        values_data->reserve(input_rows_count);
        offsets->reserve(input_rows_count);

        //auto dst = values_data->create(ColumnArray::create(ColumnUInt64::create()));
        //Array res(list_value.size());
        //PODArray<UInt64> values;

        //values_data->insertData

        
        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            //for (size_t j = 0; j < i; j++)
            //{
            //    values_data->insert(j);
            //}

            //auto dst = ColumnArray::create(ColumnUInt64::create());

            keys_data->insert(i);
            //values_data->insert(i);

            //Array values;
            //values.resize(i);

            Array values(i);

            for (size_t j = 0; j < i; j++)
            {
                //values.push_back(j);
                values[j] = j;
            }
            values_data->insert(values);
            
            keys_data->insert(i+50);
            values_data->insert(values);
            current_offset++;

            

            current_offset ++;
            offsets->insert(current_offset);
        }
        


        
        //auto dst = ColumnMap::create(ColumnUInt8::create(), ColumnArray::create(ColumnUInt8::create()), ColumnUInt8::create());
        



        //return ColumnMap::create(std::move(keys_data), std::move(values_data), std::move(offsets));


        auto nested_column
            = ColumnArray::create(ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}), std::move(offsets));

        return ColumnMap::create(nested_column);
    }
};







}

void registerzFunctionsMap(FunctionFactory & factory)
{
    factory.registerFunction<zFunctionMap>();
    factory.registerFunction<zFunctionHelloStr>();
    factory.registerFunction<zFunctionHelloInt>();
    factory.registerFunction<zFunctionHelloArray>();
    factory.registerFunction<zFunctionHelloMap>();

    
}

}
