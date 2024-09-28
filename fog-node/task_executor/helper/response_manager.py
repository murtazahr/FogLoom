# fog_node.py
import zmq
import zmq.auth
import os
import threading

class ResponseManager:
    def __init__(self, iot_address, iot_public_key):
        self.iot_address = iot_address
        self.iot_public_key = iot_public_key.encode('ascii') if isinstance(iot_public_key, str) else iot_public_key
        self._public_key, self._private_key = self._generate_keys()
        self.context = None
        self.socket = None
        self.is_connected = False

    @staticmethod
    def _generate_keys():
        keys_dir = os.path.join(os.getcwd(), 'fog_keys')
        os.makedirs(keys_dir, exist_ok=True)
        client_public_file, client_secret_file = zmq.auth.create_certificates(keys_dir, "fog_client")
        client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
        return client_public, client_secret

    def connect(self):
        if self.is_connected:
            print("FOG Node is already connected.")
            return

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.curve_secretkey = self._private_key
        self.socket.curve_publickey = self._public_key
        self.socket.curve_serverkey = self.iot_public_key
        self.socket.connect(self.iot_address)
        self.is_connected = True
        print(f"FOG Node connected to IoT device at {self.iot_address}")

    def disconnect(self):
        if not self.is_connected:
            print("FOG Node is not connected.")
            return

        self.socket.close()
        self.context.term()
        self.is_connected = False
        print("FOG Node disconnected.")

    def send_message(self, message):
        if not self.is_connected:
            print("FOG Node is not connected. Call connect() first.")
            return

        try:
            self.socket.send_string(message)
            response = self.socket.recv_string()
            print(f"Sent: '{message}', Response: {response}")
            return response
        except zmq.ZMQError as e:
            print(f"Error sending message: {e}")
            return None

    @property
    def public_key(self):
        """
        Get the public key of the FOG node.

        Returns:
            str: The public key as a string in Z85 (ZeroMQ's encoding) format.
        """
        return self._public_key.decode('ascii')

# This part is optional, allowing the script to be run standalone for testing
if __name__ == "__main__":
    import sys

    if len(sys.argv) != 4:
        print("Usage: python fog_node.py <iot_ip> <iot_port> <iot_public_key>")
        sys.exit(1)

    iot_ip = sys.argv[1]
    iot_port = sys.argv[2]
    iot_public_key = sys.argv[3]

    iot_address = f"tcp://{iot_ip}:{iot_port}"
    fog_node = ResponseManager(iot_address, iot_public_key)
    fog_node.connect()

    try:
        while True:
            message = input("Enter message to send (or 'quit' to exit): ")
            if message.lower() == 'quit':
                break
            fog_node.send_message(message)
    finally:
        fog_node.disconnect()