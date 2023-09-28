from SqlHelper import SqlHelper

def create_user_table(sqlHelper):
    query = """CREATE TABLE IF NOT EXISTS User (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                has_labels BOOLEAN)
            """
    # This adds table_name to the %s variable and executes the query
    sqlHelper.cursor.execute(query)
    sqlHelper.db_connection.commit()

def create_activity_table(sqlHelper):
    query = """CREATE TABLE IF NOT EXISTS Activity (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                user_id INT,
                transportation_mode VARCHAR(30),
                start_date_time DATETIME,
                end_date_time DATETIME,
                FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
            )
            """
    # This adds table_name to the %s variable and executes the query
    sqlHelper.cursor.execute(query)
    sqlHelper.db_connection.commit()

def create_trackpoint_table(sqlHelper):
    query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME,
                FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
            )
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
        create_activity_table(sqlHelper)
        create_trackpoint_table(sqlHelper)
        # sqlHelper.insert_data(table_name="Person", stringArray=['Bobby', 'Mc', 'McSmack', 'Board'])
        _ = sqlHelper.fetch_data(table_name="User")
        __ = sqlHelper.fetch_data(table_name="Activity")
        ___ = sqlHelper.fetch_data(table_name="TrackPoint")

        # Check that the table is dropped
        sqlHelper.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if sqlHelper:
            sqlHelper.connection.close_connection()


if __name__ == '__main__':
    main()