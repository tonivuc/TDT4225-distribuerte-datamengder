from SqlHelper import SqlHelper
import os
import traceback

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
                user_id VARCHAR(30),
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
# One activity consits of many trackpoints (each trackpoint refers to an activity, but activities are freestanding entries with a start and end time)
# Can probably insert the activities in one go, and then add the transportation modes later?
# Just make sure to exclude .plt files with more than 2500 lines (+ 6 lines of the top metadata)
# When adding the transportation mode, we have to check that the activity start time (in the mySQL row) matches the start-time in the labels.txt
def read_trackpoints(user_id):
    # Define the path to the Trajectory directory
    path = os.path.join("dataset", "dataset", "Data", user_id, "Trajectory")
    # Initialize an empty list to store the objects
    filename_and_trackpoints = {}
    
    # Loop over the .plt files in the Trajectory directory
    for filename in os.listdir(path):
        if filename.endswith(".plt"):
            filepath = os.path.join(path, filename)

            trackpoints = []
            
            # Open the file and skip the first 6 lines
            with open(filepath, "r") as f:
                for i in range(6):
                    next(f)
                
                # Read the remaining lines into objects
                for line in f:
                    if len(trackpoints) >= 2500:
                        break
                    values = line.strip().split(",")
                    obj = {
                        "latitude": float(values[0]),
                        "longitude": float(values[1]),
                        "altitude": float(values[2]),
                        "date": values[5],
                        "time": values[6]
                    }
                    trackpoints.append(obj)

        filename_and_trackpoints[filename] = trackpoints
    
    return filename_and_trackpoints

def create_activities_from_trackpoints(filename_and_trackpoints, sqlHelper: SqlHelper, user_id):
    for filename, trackpoints in filename_and_trackpoints.items():
        # Get the start and end date/time from the trackpoints
        start_date_time = trackpoints[0]["date"] + " " + trackpoints[0]["time"]
        end_date_time = trackpoints[-1]["date"] + " " + trackpoints[-1]["time"]
        
        # Insert the activity into the database
        sql = "INSERT INTO Activity (user_id, start_date_time, end_date_time) VALUES (%s, %s, %s)"
        values = (user_id, start_date_time, end_date_time)
        activity_id = sqlHelper.cursor.execute(sql, values)
        print(activity_id)

        # insert_query = """INSERT INTO Activity (user_id, start_date_time, end_date_time) VALUES ({user_id}, {start_date_time}, {end_date_time})"""

        
        
        
        # Insert the trackpoints into the database
        # for trackpoint in trackpoints:
        #     sql = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (?, ?, ?, ?, ?, ?)"
        #     values = (activity_id, trackpoint["latitude"], trackpoint["longitude"], trackpoint["altitude"], None, trackpoint["date"] + " " + trackpoint["time"])
        #     sqlHelper.cursor.execute(sql, values)

        # Commit the changes to the database
        sqlHelper.db_connection.commit()

# Trackpoint stuff

def main():
    sqlHelper = None
    try:
        # array_of_user_ids_from_file()
        sqlHelper = SqlHelper()
        # drop_all_tables(sqlHelper)
        
        # create_user_table(sqlHelper)
        # add_users_to_table(sqlHelper)
        # create_activity_table(sqlHelper)
        # create_trackpoint_table(sqlHelper)

        filename_and_trackpoints = read_trackpoints("000")
        create_activities_from_trackpoints(filename_and_trackpoints, sqlHelper, "000")
        # filename, trackpoints = next(iter(activities.items()))
        # print(trackpoints)
        # print(filename)

        # # sqlHelper.insert_data(table_name="Person", stringArray=['Bobby', 'Mc', 'McSmack', 'Board'])
        # _ = sqlHelper.fetch_data(table_name="User")
        __ = sqlHelper.fetch_data(table_name="Activity")
        # ___ = sqlHelper.fetch_data(table_name="TrackPoint")

        # # Check that the table is dropped
        # sqlHelper.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        traceback.print_exc()
    finally:
        if sqlHelper:
            sqlHelper.connection.close_connection()


if __name__ == '__main__':
    main()