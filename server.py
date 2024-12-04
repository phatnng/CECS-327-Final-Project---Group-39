import socket
import threading
import pytz
from pymongo import MongoClient
from datetime import datetime, timedelta

# Maximum packet size for communication
maxPacketSize = 1024

def listen_on_tcp(tcp_socket: socket.socket, address):
    """
    Handles communication with a connected TCP client.
    Receives queries from the client and sends back appropriate responses.
    """
    print(f"Connected to {address}")
    try:
        while True:
            # Receive a message from the client
            client_message = tcp_socket.recv(maxPacketSize).decode()
            if not client_message or client_message.lower() == 'exit':
                # Break the loop if the client disconnects or sends 'exit'
                break

            # Determine the response based on the received query
            if client_message == "What is the average moisture inside my kitchen fridge in the past three hours?":
                response = calc_avg_moisture()
            elif client_message == "What is the average water consumption per cycle in my smart dishwasher?":
                response = calc_avg_cycle()
            elif client_message == "Which device consumed more electricity among my three IoT devices?":
                response = calc_max_electricity()
            else:
                # Send an error message for invalid queries
                response = "Sorry, this query cannot be processed. Please try one of the valid queries."

            # Send the response back to the client
            tcp_socket.send(response.encode())
    finally:
        # Close the client socket and print a message when disconnected
        tcp_socket.close()
        print(f"Connection closed with {address}")

def create_tcp_socket():
    """
    Creates and binds a TCP socket for the server.
    Prompts the user for server IP and port with validation.
    """
    # Prompt for server IP, defaulting to 0.0.0.0 (all interfaces)
    server_ip = input('Enter the server IP address (or press Enter for 0.0.0.0): ') or '0.0.0.0'
    port = input("Enter the port number for the server: ")
    
    if not port.isdigit() or not 0 < int(port) < 65536:
        print("Invalid port. Please enter a number between 1 and 65535.")
        return None

    port = int(port)

    # Create the socket and bind it to the specified IP and port
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.bind((server_ip, port))
    tcp_socket.listen(5)  # Allow up to 5 pending connections
    print(f"Server listening on {server_ip}:{port}")
    return tcp_socket

def launch_tcp_threads():
    """
    Launches the TCP server and spawns a new thread for each client connection.
    """
    tcp_socket = create_tcp_socket()
    if tcp_socket is None:
        return  # Exit if socket creation or binding fails

    try:
        while True:
            # Accept a new client connection
            conn_socket, conn_address = tcp_socket.accept()
            # Start a new thread to handle the client
            threading.Thread(target=listen_on_tcp, args=(conn_socket, conn_address)).start()
    except KeyboardInterrupt:
        # Gracefully close the server socket on termination
        tcp_socket.close()

def query_collection(filter_pipeline, key_field="_id"):
    """
    Queries the MongoDB collection with the provided pipeline.
    Returns the results as a hashmap with the specified key field.
    """
    # Connect to the MongoDB server
    client = MongoClient("mongodb+srv://frankieandrade1127:AEHx7Srq0Wg1EhWg@327cluster.kkgy3.mongodb.net/?retryWrites=true&w=majority&appName=327Cluster")
    db = client["test"]
    collection = db["327IoT_virtual"]

    # Execute the aggregation pipeline and build the result hashmap
    cursor = collection.aggregate(filter_pipeline)
    data = {str(doc[key_field]): doc for doc in cursor if key_field in doc}
    client.close()
    return data

def calc_avg_moisture():
    """
    Calculates the average humidity in the kitchen fridge over the last 3 hours.
    """
    pipeline = [
        # Join with metadata collection to filter by specific devices
        {'$lookup': {
            'from': '327IoT_metadata',
            'localField': 'payload.parent_asset_uid',
            'foreignField': 'assetUid',
            'as': 'metadata'
        }},
        {'$unwind': '$metadata'},
        # Match documents for the Smart Refrigerator with humidity data
        {'$match': {
            'metadata.customAttributes.name': 'Smart Refrigerator',
            'payload.Humidity for SF1': {'$exists': True}
        }},
        # Select only necessary fields
        {'$project': {
            '_id': 1,
            'time': '$payload.timestamp',
            'Humidity': '$payload.Humidity for SF1',
        }}
    ]

    documents = query_collection(pipeline)

    # Filter documents to include only those within the last 3 hours
    current_time = datetime.now()
    cutoff_time = current_time - timedelta(hours=3)
    pst_timezone = pytz.timezone('US/Pacific')
    formatted_time = cutoff_time.astimezone(pst_timezone).strftime('%I:%M %p %Z')

    filtered_docs = {
        key: doc for key, doc in documents.items()
        if datetime.fromtimestamp(int(doc['time'])) > cutoff_time
    }

    # Calculate the average humidity
    total_humidity = sum(float(doc['Humidity']) for doc in filtered_docs.values())
    count = len(filtered_docs)

    if count == 0:
        return "No humidity data available for the Smart Refrigerator in the last 3 hours."

    avg_humidity = total_humidity / count
    return f'Average moisture of the Kitchen fridge is {avg_humidity:.2f}% RH since {formatted_time}'

def calc_max_electricity():
    """
    Determines which device used the most electricity.
    """
    pipeline = [
        {'$lookup': {
            'from': '327IoT_metadata',
            'localField': 'payload.parent_asset_uid',
            'foreignField': 'assetUid',
            'as': 'metadata'
        }},
        {'$unwind': '$metadata'},
        # Match devices of interest
        {'$match': {
            'metadata.customAttributes.name': {
                '$in': ['Smart Refrigerator', 'Smart Refrigerator 2', 'Smart Dishwasher']
            }
        }},
        # Extract electricity usage data
        {'$project': {
            '_id': 1,
            'Name': '$metadata.customAttributes.name',
            'Amps': {
                '$ifNull': [
                    '$payload.Ammeter for SF1',
                    {'$ifNull': ['$payload.Ammeter for SF2', {'$ifNull': ['$payload.Ammeter for DW', None]}]}
                ]
            }
        }}
    ]

    documents = query_collection(pipeline)

    # Calculate total electricity usage per device
    electricity_usage = {}
    for doc in documents.values():
        device_name = doc.get('Name')
        amps = doc.get('Amps')
        if device_name and amps is not None:
            electricity_usage[device_name] = electricity_usage.get(device_name, 0) + float(amps)

    # Find the device with the highest usage
    max_device = max(electricity_usage, key=electricity_usage.get)
    max_value = (((electricity_usage[max_device] / 30) * 120) / 1000)  # Convert to kWh
    return f'{max_device} used the most electricity with {max_value:.2f} kWh'

def calc_avg_cycle():
    """
    Calculates the average water consumption per cycle for the Smart Dishwasher.
    """
    pipeline = [
        {'$lookup': {
            'from': '327IoT_metadata',
            'localField': 'payload.parent_asset_uid',
            'foreignField': 'assetUid',
            'as': 'metadata'
        }},
        {'$unwind': '$metadata'},
        # Match documents for the Smart Dishwasher with water usage data
        {'$match': {
            'metadata.customAttributes.name': 'Smart Dishwasher',
            'payload.Water Sensor for DW': {'$exists': True}
        }},
        # Select only necessary fields
        {'$project': {
            '_id': 1,
            'Gallons': '$payload.Water Sensor for DW'
        }}
    ]

    documents = query_collection(pipeline)

    # Calculate the average water usage per cycle
    total_water = sum(float(doc['Gallons']) for doc in documents.values())
    count = len(documents)

    if count == 0:
        return "No data available for water consumption cycles."

    avg_water = total_water / count
    return f"Average water consumption per cycle for the Smart Dishwasher: {avg_water:.2f} gallons."

if __name__ == "__main__":
    # Start the TCP server
    launch_tcp_threads()
