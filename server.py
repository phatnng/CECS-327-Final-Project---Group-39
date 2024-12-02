import socket
import threading
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
            data_table = query_database()
            # Query processing
            if client_message == "What is the average moisture inside my kitchen fridge in the past three hours?":
                response = calc_avg_moisture(data_table)
            elif client_message == "What is the average water consumption per cycle in my smart dishwasher?":
                response = calc_avg_cycle(data_table)
            elif client_message == "Which device consumed more electricity among my three IoT devices?":
                response = calc_max_electricity(data_table)
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

def query_database():
    client = MongoClient("my_db")
    db = client["test"]
    collection = db["327IoT_virtual"]

    pipeline = [
        {
            '$lookup': {
                'from': '327IoT_metadata', 
                'localField': 'payload.parent_asset_uid', 
                'foreignField': 'assetUid', 
                'as': 'metadata'
            }
        }, {
            '$unwind': {
                'path': '$metadata'
            }
        }, {
            '$project': {
                'Name': '$metadata.customAttributes.name', 
                '_id': 1, 
                'time': 1,  
                'Amps': {
                    '$ifNull': [
                        '$payload.Ammeter for SF1', {
                            '$ifNull': [
                                '$payload.Ammeter for SF2', '$payload.Ammeter for DW'
                            ]
                        }
                    ]
                }, 
                'Fahrenheit': {
                    '$ifNull': [
                        '$payload.Thermistor for SF1', '$payload.Thermistor for SF2'
                    ]
                }, 
                'Humidity': {
                    '$ifNull': [
                        '$payload.Thermistor for SF1', '$payload.Thermistor for SF2'
                    ]
                }, 
                'Gallons': '$payload.Water Sensor for DW'
            }
        }
    ]

    cursor = collection.aggregate(pipeline)

    data_table = {}
    for document in cursor:
        key = str(document["_id"])
        data_table[key] = document

    client.close()
    return data_table


def calc_avg_moisture(data_table):
    """
    Caclulates the average Humidity in the kitchen fridge
    """
    sum = 0
    counter = 0
    current_time = datetime.now() #get the current time
    cutoff_time = current_time - timedelta(hours = 3) #used to filter time down to the last 3 hours
    for key, data in data_table.items():
        #only check the data for the last 3 hours of a specific frdge with humidity values
        if data.get('time') > cutoff_time and data.get('Name') == 'Smart Refrigerator' and data.get('Humidity') is not None:
            sum += float(data.get('Humidity'))
            counter += 1
    return f'Average moisture of the Kitchen fidge is: {sum/counter:.2f}% RH'

def calc_avg_cycle(data_table):
    """
    Calculates the average water consumption per cycle for the Smart Dishwasher.
    """
    total_water = 0
    count = 0

    for key, data in data_table.items():
        # Filter for Smart Dishwasher and ensure 'Gallons' field exists
        if data.get('Name') == 'Smart Dishwasher' and data.get('Gallons') is not None:
            total_water += float(data['Gallons'])  # Sum up the water usage
            count += 1  # Count the cycles

    if count == 0:  # Handle the case where no cycles are recorded
        return "No data available for water consumption cycles."

    # Calculate the average water consumption per cycle
    avg_water = total_water / count
    return f"Average water consumption per cycle for the smart dishwasher: {avg_water:.2f} gallons."

def calc_max_electricity(data_table):
    volts = 120 #All 3 devices use 120 volts
    time = 60 #Used to take the average amps in 1 hour
    electricity_usage = {'Fridge 1': 0, 'Fridge 2': 0, 'Dishwasher': 0}
    for key, data in data_table.items():
        if data.get('Name') == 'Smart Refrigerator' and data.get('Amps') is not None:
            electricity_usage['Fridge 1'] += float(data.get('Amps'))
        if data.get('Name') == 'Smart Refrigerator 2' and data.get('Amps') is not None:
            electricity_usage['Fridge 2'] += float(data.get('Amps'))
        if data.get('Name') == 'Smart Dishwasher' and data.get('Amps') is not None:
            electricity_usage['Dishwasher'] += float(data.get('Amps'))
    print(electricity_usage)
    max_device = max(electricity_usage, key=electricity_usage.get) #Get the name of the device that has the most amount of current flowing to it
    max_value = ((electricity_usage[max_device] / time) * volts) #Convert it to watts for power usage
    return f'{max_device} used the most electricity with {max_value} Watts'


if __name__ == "__main__":
    launch_tcp_threads()