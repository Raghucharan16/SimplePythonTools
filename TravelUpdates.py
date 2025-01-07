import redis
import json
from datetime import datetime, timedelta
import boto3
import pytz
import logging
import os
from dotenv import loadenv
loadenv()
logging.basicConfig(level=logging.DEBUG)

REDIS_HOST = os.getenv('HOST')
REDIS_PORT =  os.getenv('PORT')
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
sns_client = boto3.client('sns',region = os.getenv('Region'))
SNS_TOPIC_ARN =  os.getenv('TOPIC_ARN')
SNS_MIDPOINT_TOPIC_ARN =  os.getenv('MIDPOINT_ARN')

STATION_TO_MIDPOINT_CONFIG = {
    "HYDERABAD": {
        "wait_time": 30,  # minutes to wait after last boarding
        "midpoint": "Suryapet",
        "expected_time": 90  # minutes from last boarding to midpoint
    },
    "VIJAYAWADA": {
        "wait_time": 60,
        "midpoint": "Suryapet",
        "expected_time": 120
    },
    "BANGALORE": {
        "wait_time": 5,
        "midpoint": "Chittoor",
        "expected_time": 10
    },
    "TIRUPATI": {
        "wait_time": 10,
        "midpoint": "Chittoor",
        "expected_time": 20
    }
}

def format_datetime(dt):
    """Format a datetime object as a string in the standard format."""
    return str(dt.strftime("%Y-%m-%d %H:%M:%S"))

def parse_datetime(dt_str):
    """Parse a string in the standard format into a datetime object."""
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

def load_vehicle_status():
    try:
        data = redis_client.get('vehicle_status_dict')
        if data:
            logging.debug(msg="Data retrieved from Redis:", )  # Log the raw data
            status_data = json.loads(data)
            
            # Convert datetime strings back to datetime objects
            for vehicle_id, vehicle_data in status_data.items():
                if "last_boarding_point_time" in vehicle_data:
                    vehicle_data["last_boarding_point_time"] = parse_datetime(vehicle_data["last_boarding_point_time"])
                if "timestamp" in vehicle_data:
                    vehicle_data["timestamp"] = parse_datetime(vehicle_data["timestamp"])
            return status_data
        logging.debug(msg="No data found in Redis for 'vehicle_status_dict'.")
        return {}
    except Exception as e:
        logging.debug(msg=f"Error loading from Redis: {e}")
        return {}

def process_vehicles(vehicle_id):
    
    """Determine the current status of vehicles for both boarding and dropping points."""
    vehicle_status = []

    data = redis_client.get(vehicle_id)
    if not data:
        vehicle_status.append({
            "vehicle_id": vehicle_id,
            "status": "No data found"
        })

    vehicle_data = json.loads(data)
    journey_date = vehicle_data.get("journey_date")
    service_name = vehicle_data.get("service_name")
    boarding_points = vehicle_data.get("boarding_points", [])
    

    if not boarding_points:
        vehicle_status.append({
            "vehicle_id": vehicle_id,
            "status": "No boarding points found",
            "journey_date": journey_date,
            "service_name": service_name
        })

    # Determine boarding and dropping stations
    boarding_station = boarding_points[0].get("station_name")
    dropping_station = boarding_points[-1].get("station_name")

    # Split into boarding and dropping points
    actual_boarding_points = [
        point for point in boarding_points if point.get("station_name") == boarding_station
    ]
    actual_dropping_points = [
        point for point in boarding_points if point.get("station_name") != boarding_station
    ]

    # Calculate totals
    total_boarding_points = len(actual_boarding_points)
    total_dropping_points = len(actual_dropping_points)

    # Initialize variables
    last_covered_boarding = None
    next_boarding_to_cover = None
    boarding_order = 0
    boarding_delay = None  # Initialize boarding delay as None

    last_covered_dropping = None
    next_dropping_to_cover = None
    has_crossed_dropping = False
    dropping_order = 0
    dropping_delay = None  # Initialize dropping delay as None

    # Process boarding points
    for i, point in enumerate(actual_boarding_points):
        actual_arrival = point.get("actual_timeofarrival")
        
        if actual_arrival:
            last_covered_boarding = point
            boarding_order = i + 1
            boarding_delay = point.get("delay")  # Get the delay for covered boarding point
        else:
            next_boarding_to_cover = point
            
            break

    # Process dropping points
    for i, point in enumerate(actual_dropping_points):
        actual_arrival = point.get("actual_timeofarrival")

        if actual_arrival:
            last_covered_dropping = point
            dropping_order = i + 1
            dropping_delay = point.get("delay")  # Get the delay for covered dropping point
        else:
            next_dropping_to_cover = point
            
            break

    # Determine if all dropping points have been crossed
    if not next_dropping_to_cover and actual_dropping_points and actual_dropping_points[-1].get("actual_timeofarrival"):
        has_crossed_dropping = True

    # Determine the status field
    status = "Dropping" if bool(last_covered_boarding and last_covered_boarding == actual_boarding_points[-1]) else "Boarding"
    # Prepare output for the vehicle
    vehicle_status.append({
        "vehicle_number": vehicle_data.get('vehicle_number'),
        "From": boarding_station,
        "To": dropping_station,
        "journey_date": journey_date,
        "service_name": service_name,
        "tripId": str(vehicle_data.get("service_vehicle_id")),
        "status": status,
        "last_covered_boarding_point": last_covered_boarding.get("stop_name") if last_covered_boarding else None,
        "last_covered_boarding_point_id": last_covered_boarding.get("stop_id") if last_covered_boarding else None,
        "next_boarding_point": next_boarding_to_cover.get("stop_name") if next_boarding_to_cover else None,
        "boarding_delay": boarding_delay,  # Display delay for covered boarding point
        "boarding_order": boarding_order,
        "total_boarding_points": total_boarding_points,
        "last_covered_dropping_point": last_covered_dropping.get("stop_name") if last_covered_dropping else None,
        "last_covered_dropping_point_id": last_covered_dropping.get("stop_id") if last_covered_dropping else None,
        "next_dropping_point": next_dropping_to_cover.get("stop_name") if next_dropping_to_cover else None,
        "dropping_delay": dropping_delay,  # Display delay for covered dropping point
        "dropping_order": dropping_order,
        "total_dropping_points": total_dropping_points,
        "has_crossed_boarding_points": bool(last_covered_boarding and last_covered_boarding == actual_boarding_points[-1]),
        "midpoint_alert_sent" : False,
        "has_crossed_dropping_points": has_crossed_dropping,
        "tracking_link": f"https://fbgo.in/{vehicle_data.get('service_vehicle_id')}",
        "last_boarding_point_time": actual_boarding_points[-1].get("actual_timeofarrival") if actual_boarding_points[-1].get("actual_timeofarrival") else None
    })
    
    return vehicle_status

def save_vehicle_status(status_data):
    """Save or update vehicle status in Redis cache."""
    try:
        # Load existing data from Redis
        existing_data = redis_client.get('vehicle_status_dict')
        if existing_data:
            existing_data = json.loads(existing_data)
        else:
            existing_data = {}  # Initialize empty dictionary if no data exists

        # Merge new data with existing data
        for vehicle_id, data in status_data.items():
            # Format datetime fields
            if isinstance(data.get("last_boarding_point_time"), datetime):
                data["last_boarding_point_time"] = format_datetime(data["last_boarding_point_time"])
            if isinstance(data.get("timestamp"), datetime):
                data["timestamp"] = format_datetime(data["timestamp"])
            
            # Update or add the vehicle entry
            existing_data[vehicle_id] = data

        # Convert the merged data to JSON string
        json_data = json.dumps(existing_data)

        # Save the data back to Redis with a 24-hour expiration
        redis_client.setex('vehicle_status_dict', 86400, json_data)
        logging.debug(msg="Vehicle status saved to Redis cache.")
    except Exception as e:
        logging.debug(msg=f"Error saving to Redis: {e}")

# Function to clean up old statuses
def cleanup_old_statuses(current_time, vehicle_status_dict):
    current_time = parse_datetime(current_time)
    updated_dict = {}
    
    for vehicle_id, status_data in vehicle_status_dict.items():
        last_updated = status_data.get("timestamp")
        last_updated = parse_datetime(last_updated) if last_updated else None
        if status_data.get("status"):  # Only consider completed trips for removal
            if (current_time - last_updated).total_seconds() > 300 and last_updated is not None: 
                logging.debug(msg=f"Clearing status for Vehicle {vehicle_id} after 5 minutes.")
            else:
                updated_dict[vehicle_id] = status_data
        else:
            updated_dict[vehicle_id] = status_data  # Retain ongoing trips

    # Save the updated dictionary
    save_vehicle_status(updated_dict)
    return updated_dict

def send_sns_alert(topic_arn, message):
    try:
        # Serialize the dictionary to a JSON string
        if isinstance(message, dict):
            message = json.dumps(message, indent=4)

        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject="Vehicle Status Alert"
        )
        logging.debug(msg=f"SNS Alert Sent! Message ID: {response['MessageId']}")
    except Exception as e:
        logging.debug(msg=f"Error sending SNS alert: {e}")

def evaluate_and_notify(vehicle_ids):
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
    
    # Load the latest status from Redis
    vehicle_status_dict = load_vehicle_status()
    print("Initial vehicle_status_dict:", vehicle_status_dict)
    
    for vehicle_id in vehicle_ids:
        try:
            print("Processing vehicle ID:", vehicle_id)
            current_status = process_vehicles(vehicle_id)[0]
            previous_status = vehicle_status_dict.get(vehicle_id, {})
            print("Current status for vehicle", vehicle_id, ":", current_status)
            print("Previous status for vehicle", vehicle_id, ":", previous_status)

            # Extract current points
            last_boarding_name = current_status.get('last_covered_boarding_point')
            next_boarding_name = current_status.get('next_boarding_point')
            last_dropping_name = current_status.get('last_covered_dropping_point')
            next_dropping_name = current_status.get('next_dropping_point')
            boarding_order = current_status.get('boarding_order')
            dropping_order = current_status.get('dropping_order')
            midpoint_alert_sent = current_status.get('midpoint_alert_sent')
            station_config = STATION_TO_MIDPOINT_CONFIG.get(current_status.get('From'))
            
            if current_status.get('last_boarding_point_time') is not None:
                last_boarding_point_time = parse_datetime(current_status.get('last_boarding_point_time'))
            else:
                last_boarding_point_time = None

            # Debug: Check if previous_status is empty
            if not previous_status:
                print("No previous status found for vehicle", vehicle_id, ". Adding new entry.")
                # Debug: Print the values being added
                print("Values to be added:", {
                    "last_boarding_name": current_status.get('last_covered_boarding_point'),
                    "last_dropping_name": current_status.get('last_covered_dropping_point'),
                    "next_boarding_name": current_status.get('next_boarding_point'),
                    "next_dropping_name": current_status.get('next_dropping_point'),
                    "boarding_order" : current_status.get('boarding_order'),
                    "dropping_order" : current_status.get('dropping_order'),
                    "timestamp": current_time,
                    "status": current_status.get("has_crossed_dropping_points"),
                    "has_crossed_boarding_points": current_status.get("has_crossed_boarding_points"),
                    "midpoint_alert_sent": current_status.get("midpoint_alert_sent")
                })
                # Update vehicle_status_dict
                vehicle_status_dict[vehicle_id] = {
                    "last_boarding_name": current_status.get('last_covered_boarding_point'),
                    "last_dropping_name": current_status.get('last_covered_dropping_point'),
                    "next_boarding_name": current_status.get('next_boarding_point'),
                    "next_dropping_name": current_status.get('next_dropping_point'),
                    "boarding_order" : current_status.get('boarding_order'),
                    "dropping_order" : current_status.get('dropping_order'),
                    "timestamp": current_time,
                    "status": current_status.get("has_crossed_dropping_points"),
                    "has_crossed_boarding_points": current_status.get("has_crossed_boarding_points"),
                    "midpoint_alert_sent": current_status.get("midpoint_alert_sent")
                }
                print("Updated vehicle_status_dict:", vehicle_status_dict)
                save_vehicle_status(vehicle_status_dict)
                print("Saved vehicle_status_dict to Redis.")
            
            # Check if there has been any change in the boarding or dropping point
            if not current_status.get("has_crossed_boarding_points") and not current_status.get("has_crossed_dropping_points"):
                
                if (last_boarding_name != previous_status.get('last_boarding_name') and next_boarding_name != previous_status.get('next_boarding_name')) or (last_dropping_name != previous_status.get('last_dropping_name') and next_dropping_name != previous_status.get('next_dropping_name')):
                    if last_boarding_name != previous_status.get('last_boarding_name') and boarding_order != previous_status.get('boarding_order') and not previous_status.get("has_crossed_boarding_points"):
                        send_sns_alert(SNS_TOPIC_ARN, current_status)
                        logging.debug(msg=f"Boarding point alert sent for Vehicle {vehicle_id}.")
    
                    if last_dropping_name != previous_status.get('last_dropping_name') and dropping_order != previous_status.get('dropping_order') and not previous_status.get("has_crossed_dropping_points"):
                        send_sns_alert(SNS_TOPIC_ARN, current_status)
                        logging.debug(msg=f"Dropping point alert sent for Vehicle {vehicle_id}.")
                    
                    # Update the dictionary with the latest status
                    vehicle_status_dict[vehicle_id] = {
                        "last_boarding_name": last_boarding_name,
                        "last_dropping_name": last_dropping_name,
                        "next_boarding_name": next_boarding_name,
                        "next_dropping_name": next_dropping_name,
                        "timestamp": current_time,
                        "status": current_status.get("has_crossed_dropping_points"),
                        "has_crossed_boarding_points": current_status.get("has_crossed_boarding_points"),
                        "midpoint_alert_sent": current_status.get("midpoint_alert_sent",False)
                    }
                    print("Updated vehicle_status_dict after change:", vehicle_status_dict)
                    save_vehicle_status(vehicle_status_dict)
                    print("Saved updated vehicle_status_dict to Redis.")
                if last_boarding_point_time and last_boarding_point_time is not None and not previous_status.get("midpoint_alert_sent") :
                    minutes_since_boarding = parse_datetime(current_time) - last_boarding_point_time
                    minutes_since_boarding = minutes_since_boarding.total_seconds() / 60
                    print("Minutes since last boarding:", minutes_since_boarding)
                    logging.debug(msg=f"Minutes since last boarding: {minutes_since_boarding}")
                    if (minutes_since_boarding >= station_config["wait_time"] and not previous_status.get("midpoint_alert_sent") and minutes_since_boarding < station_config["expected_time"]):
                        
                        # Calculate time remaining to midpoint
                        minutes_to_midpoint = station_config["expected_time"] - minutes_since_boarding
                        
                        # Prepare midpoint alert message
                        midpoint_message = {
                            "alert_type": "MIDPOINT_APPROACH",
                            "tripId": current_status["tripId"],
                            "vehicle_number": current_status["vehicle_number"],
                            "service_name": current_status["service_name"],
                            "from_station": current_status["From"],
                            "to_station": current_status["To"],
                            "midpoint_location": station_config["midpoint"],
                            "minutes_to_midpoint": int(minutes_to_midpoint),
                            "journey_date": current_status["journey_date"],
                            "tracking_link": current_status["tracking_link"]
                        }
                        
                        # Send SNS alert
                        send_sns_alert(SNS_MIDPOINT_TOPIC_ARN, midpoint_message)
                        logging.debug(msg=f"Midpoint approach alert sent for Trip {current_status['tripId']}.")
                        logging.debug(msg=midpoint_message)
                        vehicle_status_dict[vehicle_id]["midpoint_alert_sent"] = True
                        save_vehicle_status(vehicle_status_dict)
                    
            else:
                logging.debug(msg=f"No change in boarding or dropping points for Vehicle {vehicle_id}.")

            cleanup_old_statuses(current_time, vehicle_status_dict)
        except Exception as e:
            print(f"Error processing vehicle {vehicle_id}: {e}")
            logging.error(f"Error processing vehicle {vehicle_id}: {e}")
vehicle_ids = [
    'TS07UM4813_t', 'TS08UL5110_t', 'TS07UM5012_t',
    'TS08UL5111_t', 'TS07UM4817_t', 'TS08UL4366_t',
    'TS08UL6618_t', 'TS07UM4814_t', 'TS08UL4364_t',
    'TS07UM4816_t', 'TS08UL5113_t', 'TS08UL4367_t',
    'TS08UL4365_t', 'TS07UM5011_t', 'TS07UM4812_t',
    'TS08UL6555_t', 'TS08UL6554_t', 'TS08UL5112_t',
    'TS07UM4815_t'
]
def lambda_handler(event,context):
    try:
        evaluate_and_notify(vehicle_ids)
        return {
            'statusCode': 200,
            'body': json.dumps('Alerts processed Successfully')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Exception occurred: {e}"
        }
