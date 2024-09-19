import psycopg2
import json
import requests
import csv
from datetime import datetime
from clickhouse_driver import Client

# ClickHouse Configuration
CLICKHOUSE_HOST = "host"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "user"
CLICKHOUSE_PASSWORD = "password"
CLICKHOUSE_DATABASE = "database"

# PostgreSQL Connection Setup
postgres_conn = psycopg2.connect(
    host="host",
    port=5432,
    user="user",
    password="password",
    dbname="database"  
)

# Initialize ClickHouse client
clickhouse_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE
)

service_keys = [38, 29]

def lambda_handler(service_keys):
    mobile = "8500358439"  # Replace with your actual mobile number
    otp = 123456  # Replace with the actual OTP you receive

    # # Send OTP
    # # send_otp_response = send_otp(mobile)
    # # if send_otp_response.status_code != 200:
    #     print(f"Failed to send OTP: {send_otp_response.text}")
    #     return

    # # Verify OTP
    # verify_otp_response = verify_otp(mobile, otp)
    # if verify_otp_response.status_code != 200:
    #     print(f"Failed to verify OTP: {verify_otp_response.text}")
    #     return

    # # Extract tokens from verify_otp_response
    # tokens = verify_otp_response.json()
    # access_token = tokens.get('accessToken')
    # refresh_token = tokens.get('refreshToken')

    # if not access_token or not refresh_token:
    #     print("Failed to obtain tokens")
    #     return

    csv_file_path = 'output.csv'
    today = str(datetime.now().strftime('%Y-%m-%d'))
    day_part = str(datetime.now().day)

    for service_key in service_keys:
        freshbus_response = create_trip(service_key, today)

        # if freshbus_response.status_code == 401:
        #     # Token expired, try refreshing
        #     refresh_response = refresh_token(refresh_token)
        #     if refresh_response.status_code == 200:
        #         new_tokens = refresh_response.json()
        #         access_token = new_tokens.get('accessToken')
        #         refresh_token = new_tokens.get('refreshToken')
        #         freshbus_response = create_trip(service_key, today, access_token)
        #     else:
        #         print(f"Failed to refresh token: {refresh_response.text}")
        #         return

        if freshbus_response.status_code == 200:
            print(f"Trip created successfully for serviceKey: {service_key}")
            
            # ... (Rest of your existing code for fetching classification label, route, etc.) ...

        else:
            print(f"Failed to create trip for serviceKey: {service_key}, Response: {freshbus_response.text}")

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda executed successfully')
    }
def send_otp(mobile):
    url = "https://api-stage.freshbus.com/admin/auth/sendotp"
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        "mobile": mobile
    }
    response = requests.post(url, headers=headers, json=payload)
    return response

def verify_otp(mobile, otp):
    url = "https://api-stage.freshbus.com/admin/auth/verifyotp"
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        "mobile": mobile,
        "otp": otp
    }
    response = requests.post(url, headers=headers, json=payload)
    return response

def refresh_token(refresh_token):
    url = "https://api-stage.freshbus.com/admin/auth/refresh-token"
    headers = {
        'Cookie': f'refresh_token={refresh_token}'
    }
    response = requests.post(url, headers=headers)
    return response

def create_trip(service_key, journey_date):
    url = f"https://api-stage.freshbus.com/admin/services/{service_key}/trips"
    headers = {
        'Content-Type': 'application/json',
        'Cookie': 'access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjMsInVzZXJUeXBlSWQiOjMsImlhdCI6MTcyNjQ4MTk1NywiZXhwIjoxNzI2NDgyODU3fQ.ioZI7-RbaZ35zjVHeHr40wxPddhSX3_hkfIFTfs28_Y; refresh_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjMsInVzZXJUeXBlSWQiOjMsImlhdCI6MTcyNjQ4MTk1NywiZXhwIjoxNzI3MDg2NzU3fQ.E2UtqMZwZWa5PqZSv4R9k7-Dj4LjBX2jMoYNQGveia8'
    }
    payload = {
        "journeyDate": journey_date
    }   
    response = requests.post(url, headers=headers, json=payload)
    return response


# Function to fetch classification label and route from ClickHouse
def fetch_classification_label_and_route(service_key, day):
    query = f"""
    SELECT Route, {day} AS classification_label
    FROM fare_classification
    WHERE ServiceKey = {service_key}
    """
    
    result = clickhouse_client.execute(query)
    
    if result:
        route = result[0][0]  # Get the 'Route'
        classification_label = result[0][1]  # Get the classification for the day (1/2/3/...)
        return classification_label, route
    else:
        return None, None
# Function to fetch tripId from PostgreSQL
def fetch_trip_id(service_key, journey_date):
    query = """
    SELECT id FROM Trips
    WHERE serviceId = %s AND journeyDate = %s
    """
    
    cursor = postgres_conn.cursor()
    cursor.execute(query, (service_key, journey_date))
    result = cursor.fetchone()
    cursor.close()
    
    if result:
        return result[0]  # Return tripId
    else:
        return None

# Function to update seat fare in PostgreSQL tripSeats table
def update_seat_fare(fare, trip_id, seatid):
    query = """
    UPDATE TripSeats
    SET fare = %s
    WHERE tripid = %s AND seatId = %s
    """
    
    cursor = postgres_conn.cursor()
    cursor.execute(query, (fare, trip_id, seatid))
    postgres_conn.commit()
    cursor.close()
    
    print(f"Updated fare for seatId: {seatid} in tripId: {trip_id} to {fare}")

# Function to update fares in tripSeats table
def update_fares_in_trip_seats(trip_id, classification_label, route, csv_file_path):
    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            csv_route = row['route']
            csv_classification = row['classification']
            csv_seatid = row['seatid']
            csv_base_fare = row['base_fare']
            
            if csv_route == route and csv_classification == str(classification_label):
                update_seat_fare(trip_id, csv_seatid, csv_base_fare)

# Close PostgreSQL connection when done
def close_connection():
    postgres_conn.close()

lambda_handler(service_keys=service_keys)
