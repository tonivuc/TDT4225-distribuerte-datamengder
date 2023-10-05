from SqlHelper import SqlHelper
import traceback
from datetime import timedelta

def fetchData(sqlHelper, query):
    sqlHelper.cursor.execute(query)
    return sqlHelper.cursor.fetchall()

def get_user_ids_with_labeled_activities(sqlHelper):
    query = """
        SELECT DISTINCT user_id
        FROM test_db.Activity 
        WHERE transportation_mode != 'None'
    """
    rows = fetchData(sqlHelper, query)
    return [row[0] for row in rows]

def find_transportation_modes_for_user(sqlHelper, user_id):
    query = """
        SELECT DISTINCT transportation_mode, user_id, id, start_date_time
        FROM test_db.Activity 
        WHERE transportation_mode != 'None'
        AND user_id = '{}'
        ORDER BY user_id, transportation_mode;
        """.format(user_id)
    rows = fetchData(sqlHelper, query)
    return [row for row in rows]

def find_longest_distances_traveled_per_transportation_mode_for_user(sqlHelper, user_id):
    print("Checking longest distance for transport modes for user: {}".format(user_id))
    user_activities_with_transportation_mode = find_transportation_modes_for_user(sqlHelper, user_id)

    # Contains transportation mode and distance like this: {'bus': 30144.46612781088, 'taxi': 8383.172444047177}
    transportation_modes_dict = {}

    # Iterate through the list and populate the dictionary
    for activity in user_activities_with_transportation_mode:
        # activity[0] is transportation_mode due to the query in find_transportation_modes_for_user
        if (activity[0] not in transportation_modes_dict):
            transportation_modes_dict[activity[0]] = 0

    total_distance_for_date = {}
    for activity in user_activities_with_transportation_mode:
        activity_id = activity[2]
        activity_start_datetime = activity[3]
        # Get the start of day daytime for activity_start_datetime
        day_start_time = activity_start_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        distance = get_distances_between_trackpoints(sqlHelper, activity_id, day_start_time)
        # If total_distance_for_date[day_start_time] is not a number yet, set it to 0
        if (day_start_time not in total_distance_for_date):
            total_distance_for_date[day_start_time] = 0
        total_distance_for_date[day_start_time] = total_distance_for_date[day_start_time] + distance
        # print("Total distance traveled for this day: {}. With transportation mode {}".format(total_distance_for_date[day_start_time], activity[0]))
        
        # If we found a date with a longer total distance than the one we already have stored, update the dictionary
        if (total_distance_for_date[day_start_time] > transportation_modes_dict[activity[0]]):
            transportation_modes_dict[activity[0]] = total_distance_for_date[day_start_time]
        
    return transportation_modes_dict

def get_all_transport_modes(sqlHelper):
    query = """
        SELECT DISTINCT transportation_mode
        FROM test_db.Activity 
        WHERE transportation_mode != 'None'
    """
    rows = fetchData(sqlHelper, query)
    return [row[0] for row in rows]

def find_distance_traveled_transportation_mode_all_users(sqlHelper):
    transport_modes = get_all_transport_modes(sqlHelper)
    transport_mode_distance_and_user_id = {}

    # Create a dictionary with the transport_mode as key, and distance and user_id as values
    for i, transport_mode in enumerate(transport_modes):
        distance = -1  # Default value
        user_id = ''    # Default value
        transport_mode_distance_and_user_id[transport_mode] = [distance, user_id]

    relevant_user_ids_including_128 = get_user_ids_with_labeled_activities(sqlHelper)
    relevant_user_ids = [user_id for user_id in relevant_user_ids_including_128 if user_id != '128']
    for user_id in relevant_user_ids:
        transport_modes_and_distance = find_longest_distances_traveled_per_transportation_mode_for_user(sqlHelper, user_id)
        for transport_mode, distance in transport_modes_and_distance.items():
            if (distance > transport_mode_distance_and_user_id[transport_mode][0]): # Checking if longer than the already stored distance
                print("New longest distance for transport mode {} for user {}: {}".format(transport_mode, user_id, distance))
                transport_mode_distance_and_user_id[transport_mode][0] = distance
                transport_mode_distance_and_user_id[transport_mode][1] = user_id

    print(transport_mode_distance_and_user_id)


def get_distances_between_trackpoints(sqlHelper, activity_id, start_datetime):
    # activity_start_datetime = datetime.strptime(activity_start_datetime, "%Y-%m-%d %H:%M:%S") # Use this if input is string
    activity_start_plus_24h = start_datetime + timedelta(hours=24)
    # The query calculates the total distance traveled between consecutive trackpoints 
    # for a specified activity and within a specified date and time range (24h from activity start)

    # The subquery gets us the next trackpoint. And the parent query calculates the distance between them.
    # Finally the parent of that one sums up all the distances in each row, giving us the total distance traveled.
    # We also round the distance as GPS isn't accurate anyway and it's nicer to display that way. Also we won't do any more calculations with it at this point.
    query = """
    SELECT 
        ROUND(SUM(distance_to_next_trackpoint)) AS total_distance
    FROM (
        SELECT 
            tp.id AS trackpoint_id,
            ST_Distance_Sphere(
                POINT(tp.lon, tp.lat),
                POINT(tp1.lon, tp1.lat)
            ) AS distance_to_next_trackpoint
        FROM (
            SELECT
                tp.id,
                tp.lon,
                tp.lat,
                LEAD(tp.id) OVER (ORDER BY tp.date_time) AS next_id
            FROM TrackPoint tp
            WHERE tp.activity_id = '{}'
            AND tp.date_time >= '{}'
            AND tp.date_time <= '{}'
        ) AS tp
        JOIN TrackPoint tp1 ON tp.next_id = tp1.id
    ) AS distances;
    """.format(activity_id, start_datetime, activity_start_plus_24h)
    rows = fetchData(sqlHelper, query)
    distance = rows[0][0]
    if (distance == None):
        distance = 0
    return distance

def main():
    sqlHelper = None
    try:
        sqlHelper = SqlHelper()
        # print(find_transportation_modes_for_user(sqlHelper, "010"))
        # print(get_distances_between_trackpoints(sqlHelper, "1187", "2008-05-18 00:00:00"))
        # find_longest_distances_traveled_per_transportation_mode_for_user(sqlHelper, "010")        

        # Main code
        find_distance_traveled_transportation_mode_all_users(sqlHelper)


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