%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

bad_chars = ["\ufffd", "\u003F"]

catalog_name = "xliidw_uat_lpl"
schema_list = ["gup", "procede", "procede_stage","swiss_genius","swiss_genius_inc","us_genius","wins","xuber"]

def check_table(schema_name, table_name, full_table_name):
    df = spark.read.table(full_table_name)
    bad_records = []
    for c in df.columns:        
        if dict(df.dtypes)[c] == "string":
            condition = None
            for ch in bad_chars:
                cond = col(c).like(f"%{ch}%")
                condition = cond if condition is None else (condition | cond)
            sample = df.filter(condition).select(c).limit(1).collect()
            if sample:                
                bad_records.append((schema_name, table_name, c, sample[0][0]))
    return bad_records

results = []

for schema_name in schema_list:
    tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
    tables = [row.tableName for row in tables_df.collect()]

    for t in tables:
        full_name = f"{catalog_name}.{schema_name}.{t}"
        results.extend(check_table(schema_name, t, full_name))

results_df = spark.createDataFrame(results, ["schema_name", "table_name", "column_name", "sample_value"])

display(results_df)
