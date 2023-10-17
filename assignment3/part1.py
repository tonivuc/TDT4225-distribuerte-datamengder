from pprint import pprint 
from MongoHelper import MongoHelper
import os
import traceback

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
        User.insert_one({ "_id": id, "has_labels": has_labels })

def get_user_ids(mongoHelper: MongoHelper):
    User = mongoHelper.db["User"]
    return [user["_id"] for user in User.find(projection=["_id"])]

def read_and_insert_activities_and_trackpoints_for_users(mongoHelper: MongoHelper):
    user_ids = get_user_ids(mongoHelper)
    pprint(user_ids)
    
    # for user_id in user_ids:
        # filename_and_trackpoints = read_trackpoints(user_id)
        # insert_activities_and_trackpoints(filename_and_trackpoints, sqlHelper, user_id) #TODO
        # print("Inserted activities and trackpoints for user %s" % user_id)

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

def main():
    mongoHelper = None
    try:
        mongoHelper = MongoHelper()
        # First-time setup code
        # create_collections(mongoHelper)

        # Inserting data
        # insert_users(mongoHelper)
        read_and_insert_activities_and_trackpoints_for_users(mongoHelper)
        mongoHelper.show_coll()
        # mongoHelper.insert_documents(collection_name="Person")
        # mongoHelper.fetch_documents(collection_name="Person")
        # mongoHelper.drop_coll(collection_name="Person")
        # program.drop_coll(collection_name='person')
        # program.drop_coll(collection_name='users')
        # Check that the table is dropped
        # mongoHelper.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        traceback.print_exc()
    finally:
        if mongoHelper:
            mongoHelper.connection.close_connection()


if __name__ == '__main__':
    main()