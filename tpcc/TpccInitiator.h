// NOTICE: this file is adapted from Cavalia
#ifndef __TPCC_INITIATOR_H__
#define __TPCC_INITIATOR_H__

#include "BenchmarkInitiator.h"
#include "Meta.h"

namespace DSMEngine {
namespace TpccBenchmark {
class TpccInitiator : public BenchmarkInitiator {
 public:
  TpccInitiator(const size_t& thread_count, ClusterConfig* config)
      : BenchmarkInitiator(thread_count, config) {
      printf("Iitialize the TpccInitiator\n");
  }
  ~TpccInitiator() {
  }

protected:
  virtual void RegisterTables(char* const storage_addr,
      const std::vector<RecordSchema*>& schemas) {
      printf("schema table count is %d\n", schemas.size());
    StorageManager storage_manager;
    storage_manager.RegisterTables(schemas, default_gallocator);
    storage_manager.Serialize(storage_addr);
  }

  virtual void RegisterSchemas(std::vector<RecordSchema*>& schemas) {
    schemas.resize(kTableCount, nullptr);
    InitItemSchema(schemas[ITEM_TABLE_ID]);
    InitWarehouseSchema(schemas[WAREHOUSE_TABLE_ID]);
    InitDistrictSchema(schemas[DISTRICT_TABLE_ID]);
    InitCustomerSchema(schemas[CUSTOMER_TABLE_ID]);
    InitOrderSchema(schemas[ORDER_TABLE_ID]);
    InitDistrictNewOrderSchema(schemas[DISTRICT_NEW_ORDER_TABLE_ID]);
    InitNewOrderSchema(schemas[NEW_ORDER_TABLE_ID]);
    InitOrderLineSchema(schemas[ORDER_LINE_TABLE_ID]);
    InitHistorySchema(schemas[HISTORY_TABLE_ID]);
    InitStockSchema(schemas[STOCK_TABLE_ID]);
  }

 public:
  static void InitItemSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("i_id", ValueType::INT));
    columns.push_back(new ColumnInfo("i_im_id", ValueType::INT));
    columns.push_back(
        new ColumnInfo("i_name", ValueType::VARCHAR, static_cast<size_t>(32)));
    columns.push_back(new ColumnInfo("i_price", ValueType::DOUBLE));
    columns.push_back(
        new ColumnInfo("i_data", ValueType::VARCHAR, static_cast<size_t>(64)));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));
    schema = new RecordSchema(ITEM_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0 };
    schema->SetPrimaryColumns(col_ids, 1);
    schema->SetPartitionColumns(col_ids, 1);
  }

  static void InitWarehouseSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("w_id", ValueType::INT));
    columns.push_back(
        new ColumnInfo("w_name", ValueType::VARCHAR, static_cast<size_t>(16)));
    columns.push_back(
        new ColumnInfo("w_street_1", ValueType::VARCHAR,
                       static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("w_street_2", ValueType::VARCHAR,
                       static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("w_city", ValueType::VARCHAR, static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("w_state", ValueType::VARCHAR, static_cast<size_t>(2)));
    columns.push_back(
        new ColumnInfo("w_zip", ValueType::VARCHAR, static_cast<size_t>(9)));
    columns.push_back(new ColumnInfo("w_tax", ValueType::DOUBLE));
    columns.push_back(new ColumnInfo("w_ytd", ValueType::DOUBLE));
    // Padding in the warehouse table to reduce the contention.
      columns.push_back(
              new ColumnInfo("padding", ValueType::VARCHAR, static_cast<size_t>(1024)));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));

      // The padding below is for the better concurrency.this can be generalized to other tables
    // with a very small number of rows.


    schema = new RecordSchema(WAREHOUSE_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0 };
    schema->SetPrimaryColumns(col_ids, 1);
    schema->SetPartitionColumns(col_ids, 1);
  }

  static void InitDistrictSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("d_id", ValueType::INT));
    columns.push_back(new ColumnInfo("d_w_id", ValueType::INT));
    columns.push_back(
        new ColumnInfo("d_name", ValueType::VARCHAR, static_cast<size_t>(16)));
    columns.push_back(
        new ColumnInfo("d_street_1", ValueType::VARCHAR,
                       static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("d_street_2", ValueType::VARCHAR,
                       static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("d_city", ValueType::VARCHAR, static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("d_state", ValueType::VARCHAR, static_cast<size_t>(2)));
    columns.push_back(
        new ColumnInfo("d_zip", ValueType::VARCHAR, static_cast<size_t>(9)));
    columns.push_back(new ColumnInfo("d_tax", ValueType::DOUBLE));
    columns.push_back(new ColumnInfo("d_ytd", ValueType::DOUBLE));
    columns.push_back(new ColumnInfo("d_next_o_id", ValueType::INT));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));

      schema = new RecordSchema(DISTRICT_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0, 1 };
    schema->SetPrimaryColumns(col_ids, 2);
    size_t par_col_ids[] = { 1 };
    schema->SetPartitionColumns(par_col_ids, 1);
  }

  static void InitCustomerSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("c_id", ValueType::INT));
    columns.push_back(new ColumnInfo("c_d_id", ValueType::INT));
    columns.push_back(new ColumnInfo("c_w_id", ValueType::INT));
    columns.push_back(
        new ColumnInfo("c_first", ValueType::VARCHAR, static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("c_middle", ValueType::VARCHAR, static_cast<size_t>(2)));
    columns.push_back(
        new ColumnInfo("c_last", ValueType::VARCHAR, static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("c_street_1", ValueType::VARCHAR,
                       static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("c_street_2", ValueType::VARCHAR,
                       static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("c_city", ValueType::VARCHAR, static_cast<size_t>(32)));
    columns.push_back(
        new ColumnInfo("c_state", ValueType::VARCHAR, static_cast<size_t>(2)));
    columns.push_back(
        new ColumnInfo("c_zip", ValueType::VARCHAR, static_cast<size_t>(9)));
    columns.push_back(
        new ColumnInfo("c_phone", ValueType::VARCHAR, static_cast<size_t>(32)));
    columns.push_back(new ColumnInfo("c_since", ValueType::INT64));
    columns.push_back(
        new ColumnInfo("c_credit", ValueType::VARCHAR, static_cast<size_t>(2)));
    columns.push_back(new ColumnInfo("c_credit_lim", ValueType::DOUBLE));
    columns.push_back(new ColumnInfo("c_discount", ValueType::DOUBLE));
    columns.push_back(new ColumnInfo("c_balance", ValueType::DOUBLE));
    columns.push_back(new ColumnInfo("c_ytd_payment", ValueType::DOUBLE));
    columns.push_back(new ColumnInfo("c_payment_cnt", ValueType::INT));
    columns.push_back(new ColumnInfo("c_delivery_cnt", ValueType::INT));
    columns.push_back(
        new ColumnInfo("c_data", ValueType::VARCHAR, static_cast<size_t>(500)));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));
    schema = new RecordSchema(CUSTOMER_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0, 1, 2 };
    schema->SetPrimaryColumns(col_ids, 3);
    size_t par_col_ids[] = { 2 };
    schema->SetPartitionColumns(par_col_ids, 1);
  }

  static void InitOrderSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("o_id", ValueType::INT));
    columns.push_back(new ColumnInfo("o_c_id", ValueType::INT));
    columns.push_back(new ColumnInfo("o_d_id", ValueType::INT));
    columns.push_back(new ColumnInfo("o_w_id", ValueType::INT));
    columns.push_back(new ColumnInfo("o_entry_d", ValueType::INT64));
    columns.push_back(new ColumnInfo("o_carrier_id", ValueType::INT));
    columns.push_back(new ColumnInfo("o_ol_cnt", ValueType::INT));
    columns.push_back(new ColumnInfo("o_all_local", ValueType::INT));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));
    schema = new RecordSchema(ORDER_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0, 2, 3 };
    schema->SetPrimaryColumns(col_ids, 3);
    size_t par_col_ids[] = { 3 };
    schema->SetPartitionColumns(par_col_ids, 1);
  }

  static void InitDistrictNewOrderSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("d_id", ValueType::INT));
    columns.push_back(new ColumnInfo("w_id", ValueType::INT));
    columns.push_back(new ColumnInfo("o_id", ValueType::INT));
    // Reduce the contention for update.
    columns.push_back(
              new ColumnInfo("c_data", ValueType::VARCHAR, static_cast<size_t>(480)));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));
    schema = new RecordSchema(DISTRICT_NEW_ORDER_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0, 1 };
    schema->SetPrimaryColumns(col_ids, 2);
    size_t par_col_ids[] = { 1 };
    schema->SetPartitionColumns(par_col_ids, 1);
  }

  static void InitNewOrderSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("o_id", ValueType::INT));
    columns.push_back(new ColumnInfo("d_id", ValueType::INT));
    columns.push_back(new ColumnInfo("w_id", ValueType::INT));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));
    schema = new RecordSchema(NEW_ORDER_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0, 1, 2 };
    schema->SetPrimaryColumns(col_ids, 3);
    size_t par_col_ids[] = { 2 };
    schema->SetPartitionColumns(par_col_ids, 1);
  }

  static void InitOrderLineSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("ol_o_id", ValueType::INT));
    columns.push_back(new ColumnInfo("ol_d_id", ValueType::INT));
    columns.push_back(new ColumnInfo("ol_w_id", ValueType::INT));
    columns.push_back(new ColumnInfo("ol_number", ValueType::INT));
    columns.push_back(new ColumnInfo("ol_i_id", ValueType::INT));
    columns.push_back(new ColumnInfo("ol_supply_w_id", ValueType::INT));
    columns.push_back(new ColumnInfo("ol_delivery_d", ValueType::INT64));
    columns.push_back(new ColumnInfo("ol_quantity", ValueType::INT));
    columns.push_back(new ColumnInfo("ol_amount", ValueType::DOUBLE));
    columns.push_back(
        new ColumnInfo("ol_dist_info", ValueType::VARCHAR,
                       static_cast<size_t>(32)));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));

      schema = new RecordSchema(ORDER_LINE_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0, 1, 2, 3 };
    schema->SetPrimaryColumns(col_ids, 4);
    //size_t sec_col_ids[] = {0,1,2};
    //order_line_schema.AddSecondaryColumns(sec_col_ids, 3, gallocator);
    size_t par_col_ids[] = { 0, 1, 2 };
    schema->SetPartitionColumns(par_col_ids, 3);
  }

  static void InitHistorySchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("h_c_id", ValueType::INT));
    columns.push_back(new ColumnInfo("h_c_d_id", ValueType::INT));
    columns.push_back(new ColumnInfo("h_c_w_id", ValueType::INT));
    columns.push_back(new ColumnInfo("h_d_id", ValueType::INT));
    columns.push_back(new ColumnInfo("h_w_id", ValueType::INT));
    columns.push_back(new ColumnInfo("h_date", ValueType::INT64));
    columns.push_back(new ColumnInfo("h_amount", ValueType::DOUBLE));
    columns.push_back(
        new ColumnInfo("h_data", ValueType::VARCHAR, static_cast<size_t>(32)));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("ReadTS", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("WriteTS", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));
    schema = new RecordSchema(HISTORY_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 3, 4, 5 };
    schema->SetPrimaryColumns(col_ids, 3);
    size_t par_col_ids[] = { 3, 4 };
    schema->SetPartitionColumns(par_col_ids, 2);
  }

  static void InitStockSchema(RecordSchema *&schema) {
    std::vector<ColumnInfo*> columns;
    columns.push_back(new ColumnInfo("s_i_id", ValueType::INT));
    columns.push_back(new ColumnInfo("s_w_id", ValueType::INT));
    columns.push_back(new ColumnInfo("s_quantity", ValueType::INT));
    for (size_t i = 0; i < 10; ++i) {
      std::string name = "s_dists" + std::to_string(i);
      columns.push_back(
          new ColumnInfo(name.c_str(), ValueType::VARCHAR,
                         static_cast<size_t>(32)));
    }
    columns.push_back(new ColumnInfo("s_ytd", ValueType::INT));
    columns.push_back(new ColumnInfo("s_order_cnt", ValueType::INT));
    columns.push_back(new ColumnInfo("s_remote_cnt", ValueType::INT));
    columns.push_back(
        new ColumnInfo("s_data", ValueType::VARCHAR, static_cast<size_t>(64)));
//#if defined(TO)
//      columns.push_back(new ColumnInfo("meta", ValueType::INT64));
//#endif
//#if defined(TO)| defined(OCC)
//      columns.push_back(new ColumnInfo("meta", ValueType::INT64));
//#endif
      columns.push_back(new ColumnInfo("meta", ValueType::META));
    schema = new RecordSchema(STOCK_TABLE_ID);
    schema->InsertColumns(columns);
    size_t col_ids[] = { 0, 1 };
    schema->SetPrimaryColumns(col_ids, 2);
    size_t par_col_ids[] = { 0 };
    schema->SetPartitionColumns(par_col_ids, 1);
  }
};
}
}

#endif
