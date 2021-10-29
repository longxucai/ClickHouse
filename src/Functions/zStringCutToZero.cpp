#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

class ZFunctionToStringCutToZero : public IFunction
{
public:
    //函数名
    static constexpr auto name = "zToStringCutToZero";
    //函数指针，作用是？？
    static FunctionPtr create(ContextPtr) { return std::make_shared<ZFunctionToStringCutToZero>(); }
    //返回函数名
    String getName() const override { return name; }
    //函数接收的参数个数
    size_t getNumberOfArguments() const override { return 1; }

    //？？
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    //对参数个数和数据类型的检查，同时返回函数返回值对应的 DataType
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {

        LOG_DEBUG(&Poco::Logger::get("ZFunctionToStringCutToZero"), "ZFunctionToStringCutToZero zzzzzzzzzzzzzzzzzzzzzz");


        if (!isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }
    //对于所有参数都为常量的情况下，是否使用系统的默认实现
    bool useDefaultImplementationForConstants() const override { return true; }

    static bool tryExecuteString(const IColumn * col, ColumnPtr & col_res)
    {
        const ColumnString * col_str_in = checkAndGetColumn<ColumnString>(col);

        if (col_str_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size());

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;

            ColumnString::Offset current_in_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                const char * pos_in = reinterpret_cast<const char *>(&in_vec[current_in_offset]);
                size_t current_size = strlen(pos_in);
                memcpySmallAllowReadWriteOverflow15(pos, pos_in, current_size);
                pos += current_size;
                *pos = '\0';
                ++pos;
                out_offsets[i] = pos - begin;
                current_in_offset = in_offsets[i];
            }
            out_vec.resize(pos - begin);

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    static bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res)
    {
        const ColumnFixedString * col_fstr_in = checkAndGetColumn<ColumnFixedString>(col);

        if (col_fstr_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_fstr_in->getChars();

            size_t size = col_fstr_in->size();

            out_offsets.resize(size);
            out_vec.resize(in_vec.size() + size);

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;
            const char * pos_in = reinterpret_cast<const char *>(in_vec.data());

            size_t n = col_fstr_in->getN();

            for (size_t i = 0; i < size; ++i)
            {
                size_t current_size = strnlen(pos_in, n);
                memcpySmallAllowReadWriteOverflow15(pos, pos_in, current_size);
                pos += current_size;
                *pos = '\0';
                out_offsets[i] = ++pos - begin;
                pos_in += n;
            }
            out_vec.resize(pos - begin);

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    //实现函数具体的执行逻辑，第一个参数为输入的参数，可通过下标访问对应的参数，第二个参数为函数返回值的类型，第三个参数为输入的行数
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IColumn * column = arguments[0].column.get();
        ColumnPtr res_column;

        if (tryExecuteFixedString(column, res_column) || tryExecuteString(column, res_column))
            return res_column;

        throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }
};


void registerZFunctionToStringCutToZero(FunctionFactory & factory)
{
    factory.registerFunction<ZFunctionToStringCutToZero>();
}

}
