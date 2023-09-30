from SqlHelper import SqlHelper
import os

# Table creation
def create_user_table(sqlHelper):
    query = """CREATE TABLE IF NOT EXISTS User ( 
                id VARCHAR(30) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN)
            """
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
    sqlHelper.cursor.execute(query)
    sqlHelper.db_connection.commit()

def drop_all_tables(sqlHelper):
    sqlHelper.drop_table("TrackPoint")
    sqlHelper.drop_table("Activity")
    sqlHelper.drop_table("User")

# User stuff
def create_map_of_user_id_and_if_they_have_labels():
    # for (root,dirs,files) in os.walk('./dataset/dataset/Data', topdown=True):
    #     print (root)
    #     print (dirs)
    #     # print (files)
    #     print ('--------------------------------')
    user_ids_with_labels = array_of_labeled_user_ids_from_file()
    user_ids = []
    data_dir = './dataset/dataset/Data'
    for subdir in os.listdir(data_dir):
        subdir_path = os.path.join(data_dir, subdir)
        if os.path.isdir(subdir_path):
            user_ids.append(subdir)
    user_ids_and_if_they_have_labels = {id: id in user_ids_with_labels for id in user_ids}

    # Print the dictionary
    print(user_ids_and_if_they_have_labels)
    return user_ids_and_if_they_have_labels

def array_of_labeled_user_ids_from_file():
    # Open the file and read its contents into a list of strings
    with open('./dataset/dataset/labeled_ids.txt', 'r') as f:
        lines = f.readlines()

    # Strip any whitespace characters from each line and store them in a new list
    ids = [line.strip() for line in lines]
    return ids

def add_users_to_table(sqlHelper):
    userMap = create_map_of_user_id_and_if_they_have_labels()
    for id, has_labels in userMap.items():
        sqlHelper.cursor.execute("INSERT INTO User (id, has_labels) VALUES (%s, %s)", (id, has_labels))

    sqlHelper.db_connection.commit()

# Activity stuff

# Trackpoint stuff

def main():
    sqlHelper = None
    try:
        # array_of_user_ids_from_file()
        sqlHelper = SqlHelper()
        
        # create_user_table(sqlHelper)
        # add_users_to_table(sqlHelper)

        # create_activity_table(sqlHelper)
        # create_trackpoint_table(sqlHelper)

        # # sqlHelper.insert_data(table_name="Person", stringArray=['Bobby', 'Mc', 'McSmack', 'Board'])
        _ = sqlHelper.fetch_data(table_name="User")
        # __ = sqlHelper.fetch_data(table_name="Activity")
        # ___ = sqlHelper.fetch_data(table_name="TrackPoint")

        # # Check that the table is dropped
        # sqlHelper.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if sqlHelper:
            sqlHelper.connection.close_connection()


if __name__ == '__main__':
    main()