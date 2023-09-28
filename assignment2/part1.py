from SqlHelper import SqlHelper

def create_user_table(sqlHelper):
    query = """CREATE TABLE IF NOT EXISTS User (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                has_labels BOOLEAN)
            """
    # This adds table_name to the %s variable and executes the query
    sqlHelper.cursor.execute(query)
    sqlHelper.db_connection.commit()

# def populate_users_table(self, table_name):
#     query = """CREATE TABLE IF NOT EXISTS %s (
#                 id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
#                 name VARCHAR(30))
#             """
#     # This adds table_name to the %s variable and executes the query
#     self.cursor.execute(query % table_name)
#     self.db_connection.commit()

def main():
    sqlHelper = None
    try:
        sqlHelper = SqlHelper()
        create_user_table(sqlHelper)
        sqlHelper.create_table(table_name="Activity")
        sqlHelper.create_table(table_name="TrackPoint")
        # sqlHelper.insert_data(table_name="Person", stringArray=['Bobby', 'Mc', 'McSmack', 'Board'])
        _ = sqlHelper.fetch_data(table_name="User")
        sqlHelper.drop_table(table_name="Activity")
        # Check that the table is dropped
        sqlHelper.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if sqlHelper:
            sqlHelper.connection.close_connection()


if __name__ == '__main__':
    main()