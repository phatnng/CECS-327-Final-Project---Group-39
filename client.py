import socket

max_packet_size = 1024

valid_queries = [
    "What is the average moisture inside my kitchen fridge in the past three hours?",
    "What is the average water consumption per cycle in my smart dishwasher?",
    "Which device consumed more electricity among my three IoT devices?"
]

def connect_to_server():
    server_ip = input("Please enter the server IP address (default is 127.0.0.1): ") or '127.0.0.1'
    try:
        tcp_port = int(input("Please enter the TCP port of the host (default is 8000): ") or 8000)
        if not 0 < tcp_port < 65536:
            raise ValueError("Port number must be between 1 and 65535.")
    except ValueError as e:
        print(f"Invalid input. Using default port 8000: {e}")
        tcp_port = 8000

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        tcp_socket.connect((server_ip, tcp_port))
        print(f"Connected to {server_ip} on port {tcp_port}")
        return tcp_socket
    except Exception as e:
        print(f"Failed to connect to {server_ip} on port {tcp_port}: {e}")
        return None

def process_query(tcp_socket):
    if tcp_socket is None:
        return  # Exit if the connection was not established

    try:
        while True:
            print("\nAvailable queries:")
            for i, query in enumerate(valid_queries, 1):
                print(f"{i}. {query}")
            print("Type 'exit' to close the client.\n")
            
            client_message = input("Enter your query number or type the query directly:\n> ").strip()

            if client_message.lower() == "exit":
                print("Closing the connection.")
                break

            # Allow user to input either a number or the query text directly
            if client_message.isdigit() and 1 <= int(client_message) <= len(valid_queries):
                query_to_send = valid_queries[int(client_message) - 1]
            elif client_message in valid_queries:
                query_to_send = client_message
            else:
                print("\nInvalid input. Please enter a valid query or query number.")
                continue

            # Send the valid query to the server
            tcp_socket.send(query_to_send.encode())
            # Receive and display the server's response
            reply = tcp_socket.recv(max_packet_size).decode()
            print("\nReceived from server:", reply)
    finally:
        tcp_socket.close()

if __name__ == "__main__":
    tcp_socket = connect_to_server()
    process_query(tcp_socket)
