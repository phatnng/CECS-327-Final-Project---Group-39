import socket

# Maximum size of packets to receive from the server
max_packet_size = 1024

# List of valid queries that the client can send to the server
valid_queries = [
    "What is the average moisture inside my kitchen fridge in the past three hours?",
    "What is the average water consumption per cycle in my smart dishwasher?",
    "Which device consumed more electricity among my three IoT devices?"
]

def connect_to_server():
    """
    Establishes a connection to the TCP server.
    Prompts the user for the server IP and port, with default values for convenience.
    """
    # Prompt the user for server IP address; default is localhost
    server_ip = input("Please enter the server IP address (default is 127.0.0.1): ") or '127.0.0.1'
    
    try:
        # Prompt the user for the TCP port number; default is 8000
        tcp_port = int(input("Please enter the TCP port of the host (default is 8000): ") or 8000)
        # Validate the port number is in the acceptable range
        if not 0 < tcp_port < 65536:
            raise ValueError("Port number must be between 1 and 65535.")
    except ValueError as e:
        # Handle invalid input and use default port if necessary
        print(f"Invalid input. Using default port 8000: {e}")
        tcp_port = 8000

    # Create a TCP socket using IPv4 addressing
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Attempt to connect to the server using the provided IP and port
        tcp_socket.connect((server_ip, tcp_port))
        print(f"Connected to {server_ip} on port {tcp_port}")
        return tcp_socket
    except Exception as e:
        # Handle connection errors and notify the user
        print(f"Failed to connect to {server_ip} on port {tcp_port}: {e}")
        return None

def process_query(tcp_socket):
    """
    Handles user interaction for sending queries to the server and displaying responses.
    Provides an interactive menu for selecting and sending queries.
    """
    if tcp_socket is None:
        # Exit the function if the socket connection was not established
        return

    try:
        while True:
            # Display the available queries to the user
            print("\nAvailable queries:")
            for i, query in enumerate(valid_queries, 1):
                print(f"{i}. {query}")
            print("Type 'exit' to close the client.\n")
            
            # Prompt the user to input a query or its number
            client_message = input("Enter your query number or type the query directly:\n> ").strip()

            if client_message.lower() == "exit":
                # Exit the loop if the user types 'exit'
                print("Closing the connection.")
                break

            # Determine the query to send based on the user's input
            if client_message.isdigit() and 1 <= int(client_message) <= len(valid_queries):
                # Input is a valid query number
                query_to_send = valid_queries[int(client_message) - 1]
            elif client_message in valid_queries:
                # Input matches a query text
                query_to_send = client_message
            else:
                # Invalid input; prompt the user again
                print("\nInvalid input. Please enter a valid query or query number.")
                continue

            # Send the selected query to the server
            tcp_socket.send(query_to_send.encode())
            
            # Receive and decode the server's response
            reply = tcp_socket.recv(max_packet_size).decode()
            print("\nReceived from server:", reply)
    finally:
        # Close the socket connection gracefully
        tcp_socket.close()

if __name__ == "__main__":
    # Main execution: establish connection and process queries
    tcp_socket = connect_to_server()
    process_query(tcp_socket)
