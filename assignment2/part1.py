from SqlHelper import SqlHelper
import os
import traceback
from datetime import datetime

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

def get_user_ids(sqlHelper):
    sqlHelper.cursor.execute("SELECT id FROM User")
    rows = sqlHelper.cursor.fetchall()
    return [row[0] for row in rows]

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
            should_add_activity_to_database = True
            
            # Open the file and skip the first 6 lines
            with open(filepath, "r") as f:
                # First 6 lines are just metadata
                for i in range(6):
                    next(f)
                
                # Read the remaining lines into objects
                for line in f:
                    # If the file has more than 2500 trackpoints, don't add more to the trackpoints array
                    # We only add activities to the database that have a maximum of 2500 trackpoints
                    if len(trackpoints) > 2500:
                        should_add_activity_to_database = False
                        break
                    values = line.strip().split(",")
                    obj = {
                        "latitude": float(values[0]),
                        "longitude": float(values[1]),
                        "altitude": float(values[3]),
                        "date": values[5],
                        "time": values[6]
                    }
                    trackpoints.append(obj)

        # If 
        if should_add_activity_to_database:
            filename_and_trackpoints[filename] = trackpoints
    return filename_and_trackpoints

def insert_activities_and_trackpoints(filename_and_trackpoints, sqlHelper: SqlHelper, user_id: str):
    labeled_users = array_of_labeled_user_ids_from_file()
    # Initialize lists for start times, end times, and transportation modes
    start_and_end_times = {}
    transportation_modes_file = {}

    # Check if user has labels
    if user_id in labeled_users:
        start_and_end_times, transportation_modes_file = readfile(user_id)

    for filename, trackpoints in filename_and_trackpoints.items():
        # Get the start and end date/time from the trackpoints
        start_date_time = trackpoints[0]["date"] + " " + trackpoints[0]["time"]
        end_date_time = trackpoints[-1]["date"] + " " + trackpoints[-1]["time"]
        
        # Convert the points above into datetime to be able to compare with labels.txt values
        start_date_time_in_datetime = datetime.strptime(start_date_time.replace('-', '/'), "%Y/%m/%d %H:%M:%S")
        end_date_time_in_datetime = datetime.strptime(end_date_time.replace('-', '/'), "%Y/%m/%d %H:%M:%S")
                
        # For activities without labels transportation mode is None
        transportation_mode = "None" 
        # Get transportation_mode for labeled users (i.e. users that has a labels.txt file)

        # Check if start time is in the dictionary
        if start_date_time_in_datetime in start_and_end_times:
            # Check if there is a match for start time and end time in the dict
            if start_and_end_times[start_date_time_in_datetime] == end_date_time_in_datetime:
                transportation_mode = transportation_modes_file[end_date_time_in_datetime]
        
        # Insert the activity into the database, now also transportation_mode
        sql = "INSERT INTO Activity (user_id, start_date_time, end_date_time, transportation_mode) VALUES (%s, %s, %s, %s)"
        values = (user_id, start_date_time, end_date_time, transportation_mode)
        sqlHelper.cursor.execute(sql, values)
        activity_id = sqlHelper.cursor.lastrowid

        
        # Batch insert the trackpoints into the database
        # Didn't include data_days, but feel free to add it. Needs to be added to filename_and_trackpoints first probably
        sql = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        values = [(activity_id, trackpoint["latitude"], trackpoint["longitude"], trackpoint["altitude"], None, trackpoint["date"] + " " + trackpoint["time"]) for trackpoint in trackpoints]
        sqlHelper.cursor.executemany(sql, values)

        # Commit the changes to the databases
        sqlHelper.db_connection.commit()

# Trackpoint stuff
def read_and_insert_activities_and_trackpoints_for_users(sqlHelper):
    user_ids = get_user_ids(sqlHelper)
    for user_id in user_ids:
        filename_and_trackpoints = read_trackpoints(user_id)
        insert_activities_and_trackpoints(filename_and_trackpoints, sqlHelper, user_id)
        print("Inserted activities and trackpoints for user %s" % user_id)

def readfile(user_id): # Maybe rename this function
    relative_path = 'dataset/dataset/Data/'+str(user_id)+'/labels.txt'
    full_path = os.path.abspath(relative_path)

    start_and_end_times = {}
    transportation_modes_file = {}

    # Read the labels.txt file line by line
    with open(full_path, 'r') as file:
        # Skip the header line if it exists
        next(file)

        # Process each line
        for line in file:
            # Split the line into parts based on spaces
            parts = line.strip().split()

            # Extract date and time components
            start_date, start_time, end_date, end_time, transportation_mode_file = parts

            # Combine date and time components into datetime objects
            start_datetime_file = datetime.strptime(f"{start_date} {start_time}", "%Y/%m/%d %H:%M:%S")
            end_datetime_file = datetime.strptime(f"{end_date} {end_time}", "%Y/%m/%d %H:%M:%S")

            # Append values to respective lists
            start_and_end_times[start_datetime_file] = end_datetime_file # So the start_datetime_key has the end_datatime as the value
            transportation_modes_file[end_datetime_file] = transportation_mode_file # The end_datetime has the transportation_mode as the value

    return start_and_end_times, transportation_modes_file

def main():
    sqlHelper = None
    try:
        sqlHelper = SqlHelper()
        
        
        # When wanting to clear the database and re-create the tables
        drop_all_tables(sqlHelper)
        create_user_table(sqlHelper)
        create_activity_table(sqlHelper)
        create_trackpoint_table(sqlHelper)

        # When wanting to reset the tables and make them empty
        sqlHelper.clear_table_contents("Activity")
        sqlHelper.reset_table_starting_id_to_0("Activity")
        sqlHelper.clear_table_contents("TrackPoint")
        sqlHelper.reset_table_starting_id_to_0("TrackPoint")

        add_users_to_table(sqlHelper)

        read_and_insert_activities_and_trackpoints_for_users(sqlHelper)
        #sqlHelper.show_tables()



        # Main code
        # read_and_insert_activities_and_trackpoints_for_users(sqlHelper)

        # sqlHelper.fetch_data(table_name="User")
        # sqlHelper.fetch_data(table_name="Activity")
        # sqlHelper.fetch_data(table_name="TrackPoint")

        # sqlHelper.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
        traceback.print_exc()
    finally:
        if sqlHelper:
            sqlHelper.connection.close_connection()


if __name__ == '__main__':
    main()