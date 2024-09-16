import csv
from datetime import datetime
from clickhouse_driver import Client
import psycopg2

def update_trip_seats_fares(service_key, trip_id):
    # ClickHouse Configuration
    CLICKHOUSE_HOST = "10.0.130.27"
    CLICKHOUSE_PORT = 9000
    CLICKHOUSE_USER = "freshbus"
    CLICKHOUSE_PASSWORD = "9JQH5i0yKj"
    CLICKHOUSE_DATABASE = "freshbus_operations"

    # PostgreSQL Configuration
    POSTGRES_HOST = "freshbus-postgres-dev-instance-1.ca3d0zcmduac.ap-south-1.rds.amazonaws.com"
    POSTGRES_PORT = 5432
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "Nj}#X6:sZR]+|XH7l?WKL2zd%m!4"
    POSTGRES_DB = "freshbus_db"

    # Initialize ClickHouse client
    clickhouse_client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )

    # Step 1: Query ClickHouse for route and classification_label
    current_date = datetime.now()
    day_of_month = current_date.day
    
    query = f"""
    SELECT Route, `{day_of_month}` AS classification_label
    FROM fare_classification
    WHERE ServiceKey = {service_key}
    """
    
    result = clickhouse_client.execute(query)
    
    if not result:
        print(f"No data found for ServiceKey {service_key}")
        return
    
    route, classification_label = result[0]
    
    # Step 2: Read the output.csv file
    fares_data = {}
    with open('output.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['route'] == route and row['classification'] == str(classification_label):
                fares_data[int(row['seatid'])] = float(row['base_fare'])
    
    if not fares_data:
        print(f"No matching fare data found for route {route} and classification {classification_label}")
        return
    
    # Step 3: Update PostgreSQL TripSeats table
    postgres_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB
    )
    
    cursor = postgres_conn.cursor()
    
    for seat_id, fare in fares_data.items():
        update_query = """
        UPDATE public."TripSeats"
        SET "fare" = %s
        WHERE "tripId" = %s AND "seatId" = %s
        """
        cursor.execute(update_query, (fare, trip_id, seat_id))
    
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()
    
    print(f"Updated fares for trip_id {trip_id}, route {route}, classification {classification_label}")

# Example usage:
update_trip_seats_fares(36, 5)
