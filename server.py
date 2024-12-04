import socket
import threading
import pytz
from pymongo import MongoClient
from datetime import datetime, timedelta
#from MongoDBConnection import query_database  # Import the database querying function

maxPacketSize = 1024


def listen_on_tcp(tcp_socket: socket.socket, address):
    print(f"Connected to {address}")
    try:
        while True:
            client_message = tcp_socket.recv(maxPacketSize).decode()
            if not client_message or client_message.lower() == 'exit':
                break

            if client_message == "What is the average moisture inside my kitchen fridge in the past three hours?":
                response = calc_avg_moisture()
            elif client_message == "What is the average water consumption per cycle in my smart dishwasher?":
                response = calc_avg_cycle()
            elif client_message == "Which device consumed more electricity among my three IoT devices?":
                response = calc_max_electricity()
            else:
                response = "Sorry, this query cannot be processed. Please try one of the valid queries."

            tcp_socket.send(response.encode())
    finally:
        tcp_socket.close()
        print(f"Connection closed with {address}")


def create_tcp_socket():
    """Creates and binds a TCP socket on the specified IP and port."""
    server_ip = input(
        'Enter the server IP address (or press Enter for 0.0.0.0): ') or '0.0.0.0'  # Listen on all interfaces if no input
    port = input("Enter the port number for the server: ")
    if not port.isdigit() or not 0 < int(port) < 65536:
        print("Invalid port. Please enter a number between 1 and 65535.")
        return None

    port = int(port)

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.bind((server_ip, port))
    tcp_socket.listen(5)
    print(f"Server listening on {server_ip}:{port}")
    return tcp_socket


def launch_tcp_threads():
    """Launches TCP server and handles connections in separate threads."""
    tcp_socket = create_tcp_socket()
    if tcp_socket is None:
        return  # Early exit if socket creation/binding fails

    try:
        while True:
            conn_socket, conn_address = tcp_socket.accept()
            threading.Thread(target=listen_on_tcp, args=(conn_socket, conn_address)).start()
    except KeyboardInterrupt:
        tcp_socket.close()

def query_collection(filter_pipeline, key_field="_id"):
    """
    Helper function to query the database with a specific pipeline and return results as a hashmap.
    """
    client = MongoClient("mongodb+srv://frankieandrade1127:AEHx7Srq0Wg1EhWg@327cluster.kkgy3.mongodb.net/?retryWrites=true&w=majority&appName=327Cluster")
    db = client["test"]
    collection = db["327IoT_virtual"]

    cursor = collection.aggregate(filter_pipeline)
    data = {str(doc[key_field]): doc for doc in cursor if key_field in doc}  # Build the hashmap
    client.close()
    return data


def calc_avg_moisture():

    pipeline = [
        {
            '$lookup': {
                'from': '327IoT_metadata',
                'localField': 'payload.parent_asset_uid',
                'foreignField': 'assetUid',
                'as': 'metadata'
            }
        },
        {'$unwind': '$metadata'},
        {
            '$match': {
                'metadata.customAttributes.name': 'Smart Refrigerator',
                'payload.Humidity for SF1': {'$exists': True}
            }
        },
        {
            '$project': {
                '_id': 1,
                'time': '$payload.timestamp',
                'Humidity': '$payload.Humidity for SF1',
            }
        }
    ]

    documents = query_collection(pipeline)

    current_time = datetime.now()
    cutoff_time = current_time - timedelta(hours=3)
    pst_timezone = pytz.timezone('US/Pacific')
    formatted_time = cutoff_time.astimezone(pst_timezone).strftime('%I:%M %p %Z')    

    filtered_docs = {
        key: doc for key, doc in documents.items()
        if datetime.fromtimestamp(int(doc['time'])) > cutoff_time
    }

    total_humidity = sum(float(doc['Humidity']) for doc in filtered_docs.values())
    count = len(filtered_docs)

    if count == 0:
        return "No humidity data available for the Smart Refrigerator in the last 3 hours."

    avg_humidity = total_humidity / count
    return f'Average moisture of the Kitchen fridge is {avg_humidity:.2f}% RH since {formatted_time}'


def calc_max_electricity():
    pipeline = [
        {
            '$lookup': {
                'from': '327IoT_metadata',
                'localField': 'payload.parent_asset_uid',
                'foreignField': 'assetUid',
                'as': 'metadata'
            }
        },
        {'$unwind': '$metadata'},
        {
            '$match': {
                'metadata.customAttributes.name': {
                    '$in': ['Smart Refrigerator', 'Smart Refrigerator 2', 'Smart Dishwasher']
                }
            }
        },
        {
            '$project': {
                '_id': 1,
                'Name': '$metadata.customAttributes.name',
                'Amps': {
                    '$ifNull': [
                        '$payload.Ammeter for SF1',
                        {
                            '$ifNull': [
                                '$payload.Ammeter for SF2',
                                {
                                    '$ifNull': ['$payload.Ammeter for DW', None]
                                }
                            ]
                        }
                    ]
                }
            }
        }
    ]

    # Query the database to retrieve electricity usage
    documents = query_collection(pipeline)

    # Dictionary to store total electricity usage for each device
    electricity_usage = {}

    # Sum up electricity usage per device using the Name field
    for doc in documents.values():
        device_name = doc.get('Name')
        amps = doc.get('Amps')
        if device_name and amps is not None:
            if device_name not in electricity_usage:
                electricity_usage[device_name] = 0
            electricity_usage[device_name] += float(amps)

    # Find the device with maximum electricity usage
    max_device = max(electricity_usage, key=electricity_usage.get)
    max_value = (((electricity_usage[max_device] / 30) * 120) / 1000)  # Convert to watts over an hour
    return f'{max_device} used the most electricity with {max_value:.2f} kWh'



def calc_avg_cycle():
    """
    Calculates the average water consumption per cycle for the Smart Dishwasher.
    """
    pipeline = [
        {
            '$lookup': {
                'from': '327IoT_metadata',
                'localField': 'payload.parent_asset_uid',
                'foreignField': 'assetUid',
                'as': 'metadata'
            }
        },
        {'$unwind': '$metadata'},
        {
            '$match': {
                'metadata.customAttributes.name': 'Smart Dishwasher',
                'payload.Water Sensor for DW': {'$exists': True}
            }
        },
        {
            '$project': {
                '_id': 1,
                'Gallons': '$payload.Water Sensor for DW'
            }
        }
    ]

    documents = query_collection(pipeline)
    total_water = sum(float(doc['Gallons']) for doc in documents.values())
    count = len(documents)

    if count == 0:
        return "No data available for water consumption cycles."

    avg_water = total_water / count
    return f"Average water consumption per cycle for the Smart Dishwasher: {avg_water:.2f} gallons."


if __name__ == "__main__":
    launch_tcp_threads()