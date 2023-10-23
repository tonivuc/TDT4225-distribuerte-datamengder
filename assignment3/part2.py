from MongoHelper import MongoHelper
import traceback
from datetime import datetime
from geopy.distance import geodesic
import time
import pprintpp


def task7(mongoHelper: MongoHelper):
    activities = mongoHelper.db.Activity.find({"user_id": "112", "transportation_mode": "walk"})
    total_distance = 0

    # Calculate total distance
    for activity in activities:
        trackpoints = mongoHelper.db.TrackPoint.find({"activity_id": activity["_id"]})

        prev_point = None
        for trackpoint in trackpoints:
            current_point = (trackpoint["lat"], trackpoint["lon"])

            if prev_point:
                distance = geodesic(prev_point, current_point).kilometers
                total_distance += distance

            prev_point = current_point

    # Print the total distance walked in 2008 by user with id=112
    print(f"Total distance walked in 2008 by user with id = 112: {total_distance:.2f} km")

def task8(mongoHelper: MongoHelper): 
    # Dictionary to store total altitude gained for each user
    user_altitude_gained = {}
    users = mongoHelper.db.User.find()

    for user in users:
        user_id = user["_id"]
        total_altitude_gained = 0

        
        activities = mongoHelper.db.Activity.find({"user_id": user_id})
        for activity in activities:
            activity_id = activity["_id"]
            prev_altitude = None

            trackpoints = mongoHelper.db.TrackPoint.find({"activity_id": activity_id}).sort("date_time")
            for trackpoint in trackpoints:
                current_altitude = trackpoint["altitude"]

                if prev_altitude is not None and current_altitude > prev_altitude:
                    total_altitude_gained += (current_altitude - prev_altitude)

                prev_altitude = current_altitude

        
        user_altitude_gained[user_id] = total_altitude_gained * 0.3048

    
    top_users = sorted(user_altitude_gained.items(), key=lambda x: x[1], reverse=True)[:20]

    # Print the results
    for user_id, altitude_gained in top_users:
        print(f"User {user_id}: Altitude Gained: {altitude_gained:.2f} meters")

def task9(mongoHelper: MongoHelper):
    user_and_no_of_invalid_activities = {}
    users = mongoHelper.db.User.find()
    
    for user in users:
        user_id = user["_id"]
        # Only activities where start_date_time and end_date_time deviate with more than 5 minutes
        activities = mongoHelper.db.Activity.find({"user_id": user_id, "$expr": {"$gt": [{ "$subtract": ["$end_date_time", "$start_date_time"]}, 300000]}})
        # Counter of invalid activities per user
        no_of_invalid_activities = 0
        
        for activity in activities:
                prev_trackpoint = None
                trackpoints = mongoHelper.db.TrackPoint.find({"activity_id": activity["_id"]}).sort("date_time")
                
                for trackpoint in trackpoints:
                    if prev_trackpoint:
                        time_diff = (trackpoint["date_time"] - prev_trackpoint["date_time"]).total_seconds()  

                        if time_diff >= 300:
                            no_of_invalid_activities += 1
                            break
                    prev_trackpoint = trackpoint
        
        if no_of_invalid_activities > 0:
            user_and_no_of_invalid_activities[user_id] = no_of_invalid_activities

    pprintpp.pprint(user_and_no_of_invalid_activities)


def task10(mongoHelper: MongoHelper):
    # Coordinates of Forbidden City of Beijing
    forbidden_city_coord = (39.916, 116.397)
    # Radius of Forbidden City of Beijing in kilometers
    radius = 0.5 
    # Users that have tracked an activity in the Forbidden City of Beijing
    users_in_forbidden_city = []
    
    nextUser = False
    users = mongoHelper.db.User.find()
    for user in users:
        user_id = user["_id"]
        activities = mongoHelper.db.Activity.find({"user_id": user_id})

        for activity in activities:
            activity_id = activity["_id"]
            trackpoints =  mongoHelper.db.TrackPoint.find({"activity_id": activity_id, "altitude": {"$gte": 144, "$lte": 200}, "lat": {"$gte": 39, "$lte": 40}, "lon": {"$gte": 116, "$lte": 117}})

            for trackpoint in trackpoints:
                coord_tp = (trackpoint["lat"], trackpoint["lon"])

                distance = geodesic(forbidden_city_coord, coord_tp).kilometers
                if distance < radius:
                    users_in_forbidden_city.append(user_id)
                    nextUser = True
                    break
            
            if nextUser:
                nextUser = False
                break

    pprintpp.pprint(users_in_forbidden_city)





def main():
    mongoHelper = None
    try:
        mongoHelper = MongoHelper()
        start_time = time.time()
        # task7(mongoHelper)
        # task8(mongoHelper)
        task9(mongoHelper)
        # task10(mongoHelper)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("\n")
        print(f"Elapsed time: {elapsed_time:.5f} seconds")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        traceback.print_exc()
    finally:
        if mongoHelper:
            mongoHelper.connection.close_connection()


if __name__ == '__main__':
    main()
