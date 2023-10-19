from pprint import pprint 
from MongoHelper import MongoHelper
import os
import traceback
from datetime import datetime
import pymongo

def create_collections(mongoHelper: MongoHelper):
    mongoHelper.create_coll(collection_name="User")
    mongoHelper.create_coll(collection_name="Activity")
    mongoHelper.create_coll(collection_name="TrackPoint")

# Reading user data (from exercise 2)
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

def insert_users(mongoHelper: MongoHelper):
    userMap = create_map_of_user_id_and_if_they_have_labels()
    User = mongoHelper.db["User"]

    for id, has_labels in userMap.items():
        User.insert_one({ "_id": id, "has_labels": has_labels, "activity_ids": [] })

def get_user_ids(mongoHelper: MongoHelper):
    User = mongoHelper.db["User"]
    # 1 means ASCENDING when it comes to sorting
    return [user["_id"] for user in User.find(projection=["_id"]).sort([("_id", 1)])]

def read_and_insert_activities_and_trackpoints_for_users(mongoHelper: MongoHelper):
    user_ids = get_user_ids(mongoHelper)

    # For debugging: Just write for one user
    # filename_and_trackpoints = read_trackpoints("001")
    # insert_activities_and_trackpoints(filename_and_trackpoints, mongoHelper, "001")
    
    for user_id in user_ids:
        filename_and_trackpoints = read_trackpoints(user_id)
        insert_activities_and_trackpoints(filename_and_trackpoints, mongoHelper, user_id)
        print("Inserted activities and trackpoints for user %s" % user_id)

def read_activity_info(user_id):
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

def insert_activities_and_trackpoints(filename_and_trackpoints, mongoHelper: MongoHelper, user_id: str):
    labeled_users = array_of_labeled_user_ids_from_file()
    # Initialize lists for start times, end times, and transportation modes
    start_and_end_times = {}
    transportation_modes_file = {}

    # Check if user has labels
    if user_id in labeled_users:
        start_and_end_times, transportation_modes_file = read_activity_info(user_id)

    for filename, trackpoints in filename_and_trackpoints.items():
        
        # Convert the points above into datetime to be able to compare with labels.txt values
        start_date_time_in_datetime = trackpoints[0]["date_time"]
        end_date_time_in_datetime = trackpoints[-1]["date_time"]
                
        # For activities without labels transportation mode is None
        transportation_mode = "None" 
        # Get transportation_mode for labeled users (i.e. users that has a labels.txt file)

        # Check if start time is in the dictionary
        if start_date_time_in_datetime in start_and_end_times:
            # Check if there is a match for start time and end time in the dict
            if start_and_end_times[start_date_time_in_datetime] == end_date_time_in_datetime:
                transportation_mode = transportation_modes_file[end_date_time_in_datetime]

        Activity = mongoHelper.db["Activity"]
        activity_insert_result = Activity.insert_one({"user_id": user_id, "start_date_time": start_date_time_in_datetime, "end_date_time": end_date_time_in_datetime, "transportation_mode": transportation_mode })
        # Also add the activity id to the array in the User document
        User = mongoHelper.db["User"]
        User.update_one({"_id": user_id}, {
            "$push": {
                "activity_ids": activity_insert_result.inserted_id
            }
        })

        
        # # Batch insert the trackpoints into the database
        TrackPoint = mongoHelper.db["TrackPoint"]
        trackpoints_to_insert = [{"activity_id": activity_insert_result.inserted_id, "lat": trackpoint["latitude"], "lon": trackpoint["longitude"], "altitude": trackpoint["altitude"], "date_time": trackpoint["date_time"]} for trackpoint in trackpoints]
        TrackPoint.insert_many(trackpoints_to_insert)

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
                    date = values[5]
                    time = values[6]
                    date_time_string = date + " " + time 
                    obj = {
                        "latitude": float(values[0]),
                        "longitude": float(values[1]),
                        "altitude": float(values[3]),
                        "date_time": datetime.strptime(date_time_string.replace('-', '/'), "%Y/%m/%d %H:%M:%S")
                    }
                    trackpoints.append(obj)

        # If 
        if should_add_activity_to_database:
            filename_and_trackpoints[filename] = trackpoints
    return filename_and_trackpoints

def clear_all_collections(mongoHelper: MongoHelper):
    mongoHelper.drop_coll(collection_name="User")
    mongoHelper.drop_coll(collection_name="Activity")
    mongoHelper.drop_coll(collection_name="TrackPoint")
    print("Collections dropped")
    mongoHelper.show_coll()

def main():
    mongoHelper = None
    try:
        mongoHelper = MongoHelper()
        # First-time setup code
        create_collections(mongoHelper)

        # Code to delete everything in the collections and reset the database
        #clear_all_collections(mongoHelper)

        # Inserting data
        insert_users(mongoHelper)
        read_and_insert_activities_and_trackpoints_for_users(mongoHelper)

        # Add index
        print("Adding index for quickly finding trackpoints that belong to a specific activity")
        mongoHelper.db["TrackPoint"].create_index([("activity_id", pymongo.ASCENDING)])
        print("Index added")
        mongoHelper.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        traceback.print_exc()
    finally:
        if mongoHelper:
            mongoHelper.connection.close_connection()


if __name__ == '__main__':
    main()