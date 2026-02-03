from pyspark.sql.functions import lit, current_timestamp
def merge_into_stage(source_df, primary_key, source_table, target_table, is_merge_enabled='N'):
    """
    Merges data from a source DataFrame into a Delta target table using audit columns.

    This function performs the following steps:
    1. Adds audit columns (`is_deleted`, `row_inserted_time`, `row_updated_time`) to the source data.
    2. Identifies new or updated records by comparing with the source data with the target table.
    3. Performs an upsert (MERGE) into the target Delta table.
    4. Identifies deleted rows (present in target but not in source) and:
        - Inserts them into a deleted rows audit table. Expects the deleted_rows table to be present.
        - Physically deletes them from the target table.

    Parameters:
        source_df (DataFrame): Dataframe with curated source data that is ready to be pushed to target.
        primary_key (str): The column used as a unique identifier for matching records. It can take any number of columns separated by a comma.
        source_table (str): The source table name in 'catalog.schema.table' format.
        target_table (str): The target table name in 'catalog.schema.table' format.

    Raises:
        Exception: If any error occurs during processing.
    """

    try:
        if is_merge_enabled == 'N':
            # Step 1: Write the dataframe in overwrite mode
            loggerObject.info(f"Overwrite for table {target_table} started")
            source_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)
            loggerObject.info(f"Overwrite for table {target_table} completed")
        else:
            # Note: Multi-table transactions are not supported as of now. The try block is used here only to catch exceptions here.

            # Step 1: Get current timestamp once
            ts = spark.sql("SELECT current_timestamp() AS now").collect()[0]["now"]
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # need to convert to string for sql.

            # Create source temp view for EXCEPT logic based on source_df (audit columns are not added yet)
            table_name = source_table.split(".")[-1]
            source_view = f"{table_name}_view"
            source_df.createOrReplaceTempView(source_view)

            # delete the below try block after testing is done:
            ####################################################
            try:
                # Try to access the table to confirm it exists
                print(spark.table(target_table))
            except Exception:
                # Table does not exist — create it and add audit columns
                # spark.sql(f"""
                #     CREATE TABLE IF NOT EXISTS {target_table}
                #     USING DELTA
                #     AS SELECT * FROM {source_table} WHERE 1 = 0
                # """)
                
                spark.sql(f"""
                    ALTER TABLE {target_table} ADD COLUMNS (
                        is_deleted INT,
                        row_inserted_time TIMESTAMP,
                        row_updated_time TIMESTAMP
                    )
                """)
                #####################################################


            # Get common columns to use in the EXCEPT logic (original columns only)
            # Get the columns of the target table
            target_table_columns = [col.lower() for col in spark.table(target_table).columns]

            # Check if "DW_Insert_Dt" column exists

            if "dw_insert_dt" in target_table_columns:
                target_columns = spark.table(target_table).drop(
                    "DW_Insert_Dt", "is_deleted", "row_inserted_time", "row_updated_time"
                ).columns
            else:
                target_columns = spark.table(target_table).drop(
                    "is_deleted", "row_inserted_time", "row_updated_time"
                ).columns
            # target_columns = spark.table(target_table).drop("DW_Insert_Dt","is_deleted", "row_inserted_time", "row_updated_time").columns
            target_columns_delete = spark.table(target_table).columns
            # common_columns = list(set(source_df.columns) & set(target_columns))
            common_columns = [col for col in target_columns if col in source_df.columns]
            common_columns_delete = [col for col in target_columns_delete if col in source_df.columns]
            common_columns_list = ", ".join([f"`{col}`" for col in common_columns])
            common_columns_list_for_deletes = ", ".join([f"t.`{col}`" for col in common_columns_delete])
            # print(common_columns_list_for_deletes) # troubleshooting

            ###################################################################
            # Identify new and updated rows
            from pyspark.sql.functions import col, concat_ws, trim, coalesce, lit, md5
            merge_view = f"{table_name}_merge_view" # we want to name the view after the table to avoid name collisions
            source_view_df=spark.sql(f"SELECT {common_columns_list} FROM {source_view}")
            target_table_df=spark.sql(f"SELECT {common_columns_list} FROM {target_table}")
            # Create 'mdf_column' column for both DataFrames: cast to string, trim, fillna('*~*'), then concat
            def build_mdf_column(df):
                df_cols = df.columns
                ignore_col_list = ["BaseCcyAmtRevaldLastRun", "RecalcdBaseCcyAmt", "BaseCcyRecalcdMovement"]
                md5_col_list = [col for col in df_cols if col not in ignore_col_list]
                cols = [trim(coalesce(col(c).cast("string"), lit("*~*"))) for c in md5_col_list]
                return df.withColumn("mdf_hash_column", md5(concat_ws("||", *cols)))
            source_view_df=build_mdf_column(source_view_df)
            target_table_df=build_mdf_column(target_table_df)        
            merge_df = source_view_df.join(target_table_df, source_view_df.mdf_hash_column == target_table_df.mdf_hash_column, how="left").filter(target_table_df.mdf_hash_column.isNull()).select([source_view_df[c] for c in common_columns])
            ##################################################################   

            # Identify new and updated rows
            # merge_view = f"{table_name}_merge_view" # we want to name the view after the table to avoid name collisions
            # spark.sql(f"""
            #     SELECT {common_columns_list} FROM {source_view}
            #     EXCEPT
            #     SELECT {common_columns_list} FROM {target_table}
            # """).createOrReplaceTempView(merge_view)

            # Add audit fields to merge_view in preparation to merge to target table
            #merge_df = spark.table(merge_view)
            merge_df_with_audit = merge_df
            # merge_df_with_audit = (
            #     merge_df
            #     .withColumn('DW_Insert_Dt', current_timestamp())
            #     .withColumn('is_deleted', lit(0))
            #     .withColumn('row_inserted_time', current_timestamp())
            #     .withColumn('row_updated_time', current_timestamp())
            # )
            # Check if "DW_Insert_Dt" column exists in the target table
            if "dw_insert_dt" in target_table_columns:
                merge_df_with_audit = (
                    merge_df
                    .withColumn('DW_Insert_Dt', current_timestamp())
                    .withColumn('is_deleted', lit(0))
                    .withColumn('row_inserted_time', current_timestamp())
                    .withColumn('row_updated_time', current_timestamp())
                )
            else:
                merge_df_with_audit = (
                    merge_df
                    .withColumn('is_deleted', lit(0))
                    .withColumn('row_inserted_time', current_timestamp())
                    .withColumn('row_updated_time', current_timestamp())
                )
            merge_df_with_audit.createOrReplaceTempView(merge_view)

            # Get columns for insert logic
            insert_columns = merge_df_with_audit.columns
            insert_columns_formatted = [f"`{col}`" for col in merge_df_with_audit.columns]
            insert_values = [f"source.`{col}`" for col in insert_columns]

            # START: Handle multiple primary key columns + audit columns
            loggerObject.info(f"Handle multiple primary key columns + audit columns.")
            pk_cols = [col.strip() for col in primary_key.split(",")]
            if "dw_insert_dt" in target_table_columns:
                excluded = pk_cols + ['DW_Insert_Dt', 'is_deleted', 'row_inserted_time', 'row_updated_time']
            else:
                excluded = pk_cols + ['is_deleted', 'row_inserted_time', 'row_updated_time']
            on_clause = " AND ".join([f"trim(upper(source.`{col}`)) = trim(upper(target.`{col}`))" for col in pk_cols])
            # END: Handle multiple primary key columns

            updatable_columns = [col for col in insert_columns if col not in excluded]
            
            update_set_clause = ",\n  ".join(
                [f"target.`{col}` = source.`{col}`" for col in updatable_columns] + [
                    "target.is_deleted = 0",
                    f"target.row_updated_time = cast('{ts_str}' AS TIMESTAMP)"
                ]
            )
            loggerObject.info(f"update_set_clause: {update_set_clause}")

            # Run final MERGE
            #print(f"Merging {merge_view} into {target_table}.")
            loggerObject.info(f"Merging {merge_view} into {target_table}.")
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {merge_view} AS source
            ON {on_clause}
            WHEN MATCHED THEN
            UPDATE SET
            {update_set_clause}
            WHEN NOT MATCHED THEN
            INSERT ({", ".join(insert_columns_formatted)})
            VALUES ({", ".join(insert_values)})
            """
            audit_df=spark.sql(merge_sql)
            #print("Merge SQL:") #troubleshooting
            #print(merge_sql) # troubleshooting
            updated_count = audit_df.select("num_updated_rows").collect()[0][0]
            inserted_count = audit_df.select("num_inserted_rows").collect()[0][0]
            loggerObject.info(f"Rows updated: {updated_count}, Rows inserted: {inserted_count}")
            loggerObject.info(f"***************************Merge Operation Completed******************************")

            # Delete Handling

            # Identify deleted rows - that are in target but not in source
            loggerObject.info(f"Delete Handling - Identify deleted rows - that are in target but not in source")
            delete_on_clause = " AND ".join([f"trim(upper(t.`{col}`)) = trim(upper(s.`{col}`))" for col in pk_cols])
            deleted_df_without_all_columns = spark.sql(f"""
                SELECT {common_columns_list_for_deletes}, 1 AS is_deleted, t.row_inserted_time  -- keep existing row_inserted_time value
                FROM {target_table} t
                LEFT ANTI JOIN {source_view} s
                ON {delete_on_clause}
            """)
            deleted_df = deleted_df_without_all_columns.withColumn('row_updated_time', lit(ts))

            # Check if any rows were deleted
            if not deleted_df.take(1):
                #print(f"No deleted rows found for {target_table}. Skipping deletion sync.")
                loggerObject.info(f"No deleted rows found for {target_table}. Skipping deletion sync.")
            else:
                # Step 3: Create temp view for deleted rows
                deleted_rows_view = f"{table_name}_deleted_rows_view"
                deleted_df.createOrReplaceTempView(deleted_rows_view)
                loggerObject.info(f"Deleting rows from {target_table}.")
                #print(f"Deleting rows from {target_table}.")

                # Step 4: Create deleted_rows tables in advance and remove the below table creation code:
                #########################################################################################
                abstract_target_table_name = target_table.split(".")[-1]
                deleted_rows_table = f"{target_table}_deleted_rows"
                tagret_table_location = spark.sql(f"DESCRIBE DETAIL {target_table}").select("location").collect()[0]["location"]
                delete_table_location = tagret_table_location.replace(abstract_target_table_name, f"{abstract_target_table_name}_deleted_rows")
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {deleted_rows_table}
                    USING DELTA
                    LOCATION '{delete_table_location}'
                    AS SELECT * FROM {target_table}
                    WHERE 1 = 0
                """)
                #spark.sql(f"""select * from {deleted_rows_view}""") # troubleshooting
                #########################################################################################

                # Step 5: Insert deleted rows into deleted_rows table
                #print(f"Inserting deleted rows into {deleted_rows_table}.")
                loggerObject.info(f"Inserting deleted rows into {deleted_rows_table}.")
                merge_on_deleted = " AND ".join([f"trim(upper(t.`{col}`)) = trim(upper(d.`{col}`))" for col in pk_cols])
                spark.sql(f"""
                    MERGE INTO {deleted_rows_table} t
                    USING {deleted_rows_view} d
                    ON {merge_on_deleted}
                    WHEN NOT MATCHED THEN
                    INSERT *
                """)

                #print(f"Deleting deleted rows from {target_table}.")
                loggerObject.info(f"Deleting deleted rows from {target_table}.")
                delete_merge_on = " AND ".join([f"trim(upper(t.`{col}`)) = trim(upper(d.`{col}`))" for col in pk_cols])
                spark.sql(f"""
                    MERGE INTO {target_table} t
                    USING {deleted_rows_view} d
                    ON {delete_merge_on}
                    WHEN MATCHED THEN
                    DELETE
                """)

                #print(f"Deleting already deleted rows from {deleted_rows_table} based on merge keys if records getting re-inserting again")
                loggerObject.info(f"Deleting deleted rows from {deleted_rows_table}.")
                delete_merge_on = " AND ".join([f"trim(upper(t.`{col}`)) = trim(upper(d.`{col}`))" for col in pk_cols])
                spark.sql(f"""
                    MERGE INTO {deleted_rows_table} t
                    USING {target_table} d
                    ON {delete_merge_on}
                    WHEN MATCHED THEN
                    DELETE
                """)
    except Exception as e:
        print(f"Error during merge_to_stage: {e}")
        raise