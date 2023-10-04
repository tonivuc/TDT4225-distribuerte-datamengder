from SqlHelper import SqlHelper
import traceback
from tabulate import tabulate

def main():
    sqlHelper = None
    try:
        sqlHelper = SqlHelper()

        # Main code


        # _ = sqlHelper.fetch_data(table_name="User")
        # __ = sqlHelper.fetch_data(table_name="Activity")
        # ___ = sqlHelper.fetch_data(table_name="TrackPoint")


    except Exception as e:
        print("ERROR: Failed to use database:", e)
        traceback.print_exc()
    finally:
        if sqlHelper:
            sqlHelper.connection.close_connection()


if __name__ == '__main__':
    main()