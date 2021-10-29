#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
}

class FunctionStrLen : public IFunction
{
public:
    //函数名
    static constexpr auto name = "strLen";
    //函数指针，作用是？？
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStrLen>(); }
    //返回函数名
    String getName() const override { return name; }
    //函数接收的参数个数
    size_t getNumberOfArguments() const override { return 1; }

    //？？
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    //对参数个数和数据类型的检查，同时返回函数返回值对应的 DataType
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        LOG_DEBUG(&Poco::Logger::get("FunctionStrLen"), "FunctionStrLen zzzzzzzzzzzzzzzzzzzzzz");


        if (!isString(arguments[0]))
        throw Exception(
            "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }
    //对于所有参数都为常量的情况下，是否使用系统的默认实现
    bool useDefaultImplementationForConstants() const override { return true; }

 

    //实现函数具体的执行逻辑，第一个参数为输入的参数，可通过下标访问对应的参数，第二个参数为函数返回值的类型，第三个参数为输入的行数
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        LOG_DEBUG(&Poco::Logger::get("FunctionStrLen"), "input_rows_count={}", input_rows_count);

        const auto & strcolumn = arguments[0].column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(strcolumn.get()))
        {
            const auto & offsets = col->getOffsets();
            const auto & in_vec = col->getChars();

            auto col_res = ColumnVector<UInt64>::create();
            auto & res_data = col_res->getData();
            res_data.resize(offsets.size());

            LOG_DEBUG(
                &Poco::Logger::get("FunctionStrLen"),
                "offsets.size={},offsets[-1]={},offsets[-2]={}",
                offsets.size(),
                offsets[-1],
                offsets[-2]);

            for (size_t i = 0; i < offsets.size(); ++i)
            {
                //const char * pos_in = reinterpret_cast<const char *>(&in_vec[i]);

                res_data[i] = offsets[i] - offsets[i - 1] - 1;
                LOG_DEBUG(
                    &Poco::Logger::get("FunctionStrLen"),
                    "i={},offsets[i]={},offsets[i - 1]={},res_data[i]={},out_vec[i]={}",
                    i,
                    offsets[i],
                    offsets[i - 1],
                    res_data[i],
                    reinterpret_cast<const char *>(&in_vec[offsets[i - 1]]));
            }

            return col_res;
        }
        throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }
};


void registerFunctionStrLen(FunctionFactory & factory)
{
    factory.registerFunction<FunctionStrLen>();
}

}
