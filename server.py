import socket
import threading
from pymongo import MongoClient
#from MongoDBConnection import query_database  # Import the database querying function

maxPacketSize = 1024


def listen_on_tcp(tcp_socket: socket.socket, address):
    print(f"Connected to {address}")
    try:
        while True:
            client_message = tcp_socket.recv(maxPacketSize).decode()
            if not client_message or client_message.lower() == 'exit':
                break

            # Query processing
            if client_message == "What is the average moisture inside my kitchen fridge in the past three hours?":
                response = query_database("fridge_moisture", "3_hours_avg")
            elif client_message == "What is the average water consumption per cycle in my smart dishwasher?":
                response = query_database("dishwasher", "water_cycle_avg")
            elif client_message == "Which device consumed more electricity among my three IoT devices?":
                response = query_database("electricity_consumption", "compare_devices")
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

#def query_database():



# def calc_avg_moisture(data_table):

# def calc_avg_cycle(data_table):

# def calc_max_electricity(data_table):


if __name__ == "__main__":
    launch_tcp_threads()