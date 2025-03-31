#include <iostream>
#include <glog/logging.h>
#include <gtest/gtest.h>

int main(int argc, char *argv[]) {

    testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging("BatchX");
    FLAGS_log_dir = "../../BatchX/log";
    FLAGS_alsologtostderr = true;

    LOG(INFO) << "Start test...\n";
    return RUN_ALL_TESTS();
}
//
//#include <iostream>
//#include <memory>
//#include <arrow/api.h>
//#include <arrow/csv/api.h>
//#include  "io/reader/csv_reader.h"
//void PrintTable(const std::shared_ptr<arrow::Table>& table) {
//    if (!table) {
//        std::cerr << "Table is null!" << std::endl;
//        return;
//    }
//
//    // 获取表格的 Schema（列名和数据类型）
//    std::shared_ptr<arrow::Schema> schema = table->schema();
//    std::cout << "Number of columns: " << table->num_columns() << std::endl;
//    std::cout << "Number of rows: " << table->num_rows() << std::endl;
//
//    // 遍历每一列
//    for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
//        std::string col_name = schema->field(col_idx)->name();
//        std::shared_ptr<arrow::ChunkedArray> column = table->column(col_idx);
//        std::cout << "Column '" << col_name << "' (type: " << column->type()->ToString() << "):" << std::endl;
//
//        // 遍历每一行
//        for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
//            // 获取当前行的值
//            // 注意：这里需要根据列的类型进行不同的处理
//            if (column->type()->id() == arrow::Type::INT32) {
//                auto int_column = std::static_pointer_cast<arrow::Int32Array>(column->chunk(0));
//                if (!int_column->IsNull(row_idx)) {
//                    std::cout << "  Row " << row_idx << ": " << int_column->Value(row_idx) << std::endl;
//                } else {
//                    std::cout << "  Row " << row_idx << ": NULL" << std::endl;
//                }
//            } else if (column->type()->id() == arrow::Type::STRING) {
//                auto string_column = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
//                if (!string_column->IsNull(row_idx)) {
//                    std::cout << "  Row " << row_idx << ": " << string_column->GetString(row_idx) << std::endl;
//                } else {
//                    std::cout << "  Row " << row_idx << ": NULL" << std::endl;
//                }
//            }
//            // 其他数据类型的处理可以继续添加
//        }
//    }
//}
//
//int main() {
//    // 假设你已经有了 CSVReader 对象并调用了 ReadAll
//    batchx::CSVReader csv_reader;
//    if (!csv_reader.Initialize("/Users/lihaobo/Desktop/test.csv")) {
//        std::cerr << "Failed to initialize CSVReader!" << std::endl;
//        return 1;
//    }
//
//    std::shared_ptr<arrow::Table> table = csv_reader.ReadAll();
//    if (!table) {
//        std::cerr << "Failed to read CSV file!" << std::endl;
//        return 1;
//    }
//
//    // 打印表格内容s
//    PrintTable(table);
//
//    return 0;
//}
//#include <arrow/api.h>
//#include <arrow/io/api.h>
//#include <arrow/csv/api.h>
//#include <arrow/result.h>
//#include <arrow/status.h>
//
//#include <iostream>
//#include <memory>
//
//int main() {
//    // 初始化 Arrow
//    //arrow::Status status;
//
//    // 打开CSV文件作为输入流
//    std::shared_ptr<arrow::io::InputStream> input;
//    auto status = arrow::io::ReadableFile::Open("/Users/lihaobo/Desktop/test.csv").Value(&input);
//    if (!status.ok()) {
//        std::cerr << "Failed to open CSV file: " << status.ToString() << std::endl;
//        return 1;
//    }
//
//    // 设置CSV读取选项
//    auto read_options = arrow::csv::ReadOptions::Defaults();
//    read_options.autogenerate_column_names = false;
//
//    auto parse_options = arrow::csv::ParseOptions::Defaults();
//    auto convert_options = arrow::csv::ConvertOptions::Defaults();
//    arrow::io::IOContext io_context(arrow::default_memory_pool());
//    // 创建CSV读取器
//    auto maybe_reader = arrow::csv::TableReader::Make(
//            io_context,
//            input,
//            read_options,
//            parse_options,
//            convert_options
//    );
//
//    if (!maybe_reader.ok()) {
//        std::cerr << "Failed to create CSV reader: " << maybe_reader.status().ToString() << std::endl;
//        return 1;
//    }
//
//    std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;
//
//    // 读取CSV为Arrow Table
//    auto maybe_table = reader->Read();
//    if (!maybe_table.ok()) {
//        std::cerr << "Failed to read CSV: " << maybe_table.status().ToString() << std::endl;
//        return 1;
//    }
//
//    std::shared_ptr<arrow::Table> table = *maybe_table;
//
//    // 打印表的列数和行数
//    std::cout << "Read CSV into Arrow Table with "
//              << table->num_columns() << " columns and "
//              << table->num_rows() << " rows." << std::endl;
//
//    // 可选：打印表结构
//    std::cout << "Schema: " << table->schema()->ToString() << std::endl;
//
//    return 0;
//}