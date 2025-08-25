from config.db import connect
from typing import Tuple, List, Any, Optional, Dict
from utils.response import createResponse, createDbResponse


def select_data(table: str, select: Optional[str] = None, condition: Optional[str] = None,order_by:Optional[str]=None) -> Optional[List[Tuple]]:
    try:
        select_clause = select if select else "*"
        total_query = f"SELECT {select_clause} FROM {table}"
        if condition:
            total_query += f" WHERE {condition}"
        if order_by:
            total_query += f" ORDER BY {order_by}"
        conne = connect()
        print(">>>>>>>>>>>>>>>>>>>>",total_query)
        cursor = conne.cursor()
        cursor.execute(total_query)
        records = cursor.fetchall()
        print("///////////////////////",records)
        result = createDbResponse(records, cursor.column_names, 1)
        return result
    except Exception as e:
        print(e)
        if conne:
            conne.rollback()  # Rollback the transaction if an error occurs
        raise e
    finally:
        if cursor:
            cursor.close()
        if conne:
            conne.close() 
            
            

            
def select_one_data(table: str, select: Optional[str] = None, condition: Optional[str] = None, order_by: Optional[str] = None) -> Optional[Tuple]:
    conne = None
    cursor = None
    try:
        select_clause = select if select else "*"
        total_query = f"SELECT {select_clause} FROM {table}"
        if condition:
            total_query += f" WHERE {condition}"
        if order_by:
            total_query += f" ORDER BY {order_by}"
            
        total_query += f" LIMIT 1"
        
        print(total_query)
        conne = connect()  # Assuming connect() is defined somewhere to create a database connection
        cursor = conne.cursor()
        cursor.execute(total_query)
        records = cursor.fetchone()
        result = createResponse(records, cursor.column_names, 0)

        return result
    except Exception as e:
        print(e)
        if conne:
            conne.rollback()  # Rollback the transaction if an error occurs
        raise e
    finally:
        if cursor:
            # Consume all results to avoid "Unread result found" error
            cursor.fetchall()
            cursor.close()
        if conne:
            conne.close()
            
            

def select_last_data(table: str, select: Optional[str] = None, condition: Optional[str] = None,order_by:Optional[str]=None) -> Optional[Tuple]:
    try:
        select_clause = select if select else "*"
        total_query = f"SELECT {select_clause} FROM {table}"
        if condition:
            total_query += f" WHERE {condition}"
        if order_by:
            total_query += f" ORDER BY {order_by} DESC"
        total_query += f" LIMIT 1"
        print(total_query)
        conne = connect()
        cursor = conne.cursor()
        cursor.execute(total_query)
        records = cursor.fetchone()
        result = createResponse(records, cursor.column_names, 0)
       
        print(result)
        if records:
            return result
    except Exception as e:
        print(e)
        if conne:
            conne.rollback()  # Rollback the transaction if an error occurs
        raise e
    finally:
        if cursor:
            cursor.close()
        if conne:
            conne.close()

    
    
def insert_data(table: str, column: str, row_data) -> Optional[int]:
    conne = None
    cursor = None
    try:
        conne = connect()
        cursor = conne.cursor()
        conne.start_transaction()
        query = f"INSERT INTO {table} ({column}) VALUES ({row_data})"
        print(query)
        cursor.execute(query)
        conne.commit()  # Commit the transaction
        inserted_id = cursor.lastrowid  # Get the ID of the last inserted row
        return inserted_id
    except Exception as e:
        if conne:
            conne.rollback()  # Rollback the transaction if an error occurs
        raise e
    finally:
        if cursor:
            cursor.close()
        if conne:
            conne.close() 

def batch_insert_data(table: str, columns: str, rows_data: list) -> Optional[int]:
    try:
        conn = connect()
        cursor = conn.cursor()
        conn.start_transaction()
        placeholders = ', '.join(['(' + ', '.join(['%s' for _ in range(len(rows_data[0]))]) + ')' for _ in range(len(rows_data))])
        values = [tuple(row.values()) for row in rows_data]
        flattened_values = [item for sublist in values for item in sublist]  # Flatten the list of tuples
        query = f"INSERT INTO {table} ({columns}) VALUES {placeholders}"
        
        print(query)
        cursor.execute(query, flattened_values)
        conn.commit()  # Commit the transaction
        # Get the IDs of the inserted rows
        inserted_ids = [cursor.lastrowid - i for i in range(len(rows_data))]
        
        return inserted_ids
    except Exception as e:
        print(e)
        if 'conn' in locals():
            conn.rollback()  # Rollback the transaction if an error occurs
        return e
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# rows_data = [{"name": "John", "email": "john@example.com"}, {"name": "Alice", "email": "alice@example.com"}]
# columns = 'name, email'
# table = 'your_table'
# inserted_ids = insert_data(table, columns, rows_data)
            

def delete_data(table: str, condition: str) -> bool:
    try:
        conn = connect()
        cursor = conn.cursor()
        conn.start_transaction()
        query = f"DELETE FROM {table}  WHERE {condition}"
        print(query)
        cursor.execute(query)
        row_count = cursor.rowcount
        conn.commit()  # Commit the transaction
        # return True  # Indicate successful deletion
        return row_count
    except Exception as e:
        print(e)
        if 'conn' in locals():
            conn.rollback()  # Rollback the transaction if an error occurs
        return False  # Indicate failure
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



def delete_insert_restore(original_table : str, backup_table : str, condition: str) -> bool:
    try:
        conn = connect()
        cursor = conn.cursor()
        conn.start_transaction()
        # Step 2: Insert data into another table
        insert_query = f"INSERT INTO {backup_table} SELECT * FROM {original_table} WHERE {condition}"  # Copy data to backup table
        cursor.execute(insert_query)
         # Step 1: Delete data from a table
        delete_query = f"DELETE FROM {original_table} WHERE {condition}"  # Modify <your_condition> accordingly
        cursor.execute(delete_query)
        # Commit the transaction
        conn.commit()
        return True  # Indicate success
    except Exception as e:
        print(e)
        if 'conn' in locals():
            conn.rollback()  # Rollback the transaction if an error occurs
        return False  # Indicate failure
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def update_data(table: str, set_values: dict, condition: str) -> bool:
    try:
        conn = connect()
        cursor = conn.cursor()
        conn.start_transaction()

        # Construct the SET part of the query
        set_clause = ', '.join([f"`{key}` = %s" for key in set_values.keys()])
        set_values_list = list(set_values.values())

        # Construct the UPDATE query
        query = f"UPDATE `{table}` SET {set_clause} WHERE {condition}"
        
        print(query)
        print(set_values_list)

        # Execute the UPDATE query
        cursor.execute(query, set_values_list)
        records = cursor.fetchall()
        result = createDbResponse(records, cursor.column_names, 1)

        # Commit the transaction
        conn.commit()
        return result
    except Exception as e:
        print(e)
        if 'conn' in locals():
            conn.rollback()  # Rollback the transaction if an error occurs
        return False  # Indicate failure
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# update_success = update_data("your_table", {"column1": "new_value1", "column2": "new_value2"}, "your_condition")
            

def count_rows(table: str, condition: str = "") -> int:
    try:
        conn = connect()
        cursor = conn.cursor()

        # Construct the COUNT query
        query = f"SELECT COUNT(*) FROM `{table}`"
        if condition:
            query += f" WHERE {condition}"
        # Execute the COUNT query
        cursor.execute(query)
        # Fetch the result
        row_count = cursor.fetchone()[0]
        return row_count
    except Exception as e:
        print(e)
        return -1  # Return -1 to indicate failure
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()






def select_data_in_ranges(select: str, table: str, start: int, end: int, condition: str = "")-> Tuple[Optional[int], Optional[List[Tuple[Any]]], Any, Optional[int], Optional[int]]:
    try:
        conn = connect()
        cursor = conn.cursor()
        # Construct the SELECT query
        query = f"SELECT {select} FROM `{table}`"
        if condition:
            query += f" WHERE {condition}"
        # Add LIMIT and OFFSET clauses based on the provided start and end values
        query += f" LIMIT {end - start + 1} OFFSET {start - 1}"
        # Execute the SELECT query
        cursor.execute(query)
        # Fetch row data
        row_data = cursor.fetchall()
        # Construct the query to get the total count of data
        total_query = f"SELECT COUNT(*) FROM `{table}`"
        if condition:
            total_query += f" WHERE {condition}"
        # Execute the total count query
        cursor.execute(total_query)
        total_count = cursor.fetchone()[0]
        # Fetch end data
        end_data = row_data[-1] if row_data else None
        return {"total_count": total_count, "row_data": row_data, "end_data": end_data, "start": start, "end": end}
    except Exception as e:
        print(e)
        return {"total_count": None, "row_data": None, "end_data": None, "start": None, "end": None}
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()





def custom_select_sql_query(sql,fetch_type=None):
    try:
        print(sql)
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(sql)
        if fetch_type is not None :
            records = cursor.fetchall()
            result = createDbResponse(records, cursor.column_names, 1)
        else:
            records = cursor.fetchone()
            result = createResponse(records, cursor.column_names, 0)
        return result
    except Exception as e:
        print(e)
        if conn:
            conn.rollback()  # Rollback the transaction if an error occurs
        raise e
    finally:
        if cursor:
            # Consume all results to avoid "Unread result found" error
            cursor.fetchall()
            cursor.close()
        if conn:
            conn.close()