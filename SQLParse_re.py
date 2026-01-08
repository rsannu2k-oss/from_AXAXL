
import re
import os
def parse_stored_procedure(file_path):
    with open(file_path, 'r') as file:
        sql_code = file.read()

    select_pattern = r'SELECT\s+/*?FROM\s(\w+)'
    insert_pattern = r'INSERT\s+/*?INTO\s+(\w+)'
    update_pattern = r'update\s+(\w+)'
    delete_pattern = r'DELETE FROM\s+(\w+)'

    select_statements = set(re.findall(select_pattern, sql_code, re.IGNORECASE))
    insert_statements = set(re.findall(insert_pattern, sql_code, re.IGNORECASE))
    update_statements = set(re.findall(update_pattern, sql_code, re.IGNORECASE))
    delete_statements = set(re.findall(delete_pattern, sql_code, re.IGNORECASE))

    return {
        "SELECT": select_statements,
        "INSERT": insert_statements,
        "UPDATE": update_statements,
        "DELETE": delete_statements
    }

result = parse_stored_procedure("C:\\tmp\\INFA Exports\\spu_surr_key_generator.txt")
print(result)


