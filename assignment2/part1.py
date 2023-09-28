from SqlHelper import SqlHelper

def main():
    sqlHelper = None
    try:
        sqlHelper = SqlHelper()
        sqlHelper.create_table(table_name="Person")
        sqlHelper.insert_data(table_name="Person", stringArray=['Bobby', 'Mc', 'McSmack', 'Board'])
        _ = sqlHelper.fetch_data(table_name="Person")
        sqlHelper.drop_table(table_name="Person")
        # Check that the table is dropped
        sqlHelper.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if sqlHelper:
            sqlHelper.connection.close_connection()


if __name__ == '__main__':
    main()