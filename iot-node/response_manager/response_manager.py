# iot_device.py
import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator
import os
import threading


class IoTDeviceManager:
    def __init__(self, port):
        self.port = port
        self._public_key, self._private_key = self._generate_keys()
        self.is_running = False
        self.thread = None
        self.context = None
        self.socket = None

    @staticmethod
    def _generate_keys():
        keys_dir = os.path.join(os.getcwd(), 'keys')
        os.makedirs(keys_dir, exist_ok=True)
        server_public_file, server_secret_file = zmq.auth.create_certificates(keys_dir, "server")
        server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
        return server_public, server_secret

    def start(self):
        if self.is_running:
            print("IoT Device is already running.")
            return

        self.is_running = True
        self.thread = threading.Thread(target=self._run)
        self.thread.start()
        print(f"IoT Device started on port {self.port}")
        print(f"Public key: {self.public_key}")

    def stop(self):
        if not self.is_running:
            print("IoT Device is not running.")
            return

        self.is_running = False
        if self.socket:
            self.socket.close()
        if self.context:
            self.context.term()
        if self.thread:
            self.thread.join()
        print("IoT Device stopped.")

    def _run(self):
        self.context = zmq.Context()

        # Start an authenticator for this context
        auth = ThreadAuthenticator(self.context)
        auth.start()
        auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)

        self.socket = self.context.socket(zmq.REP)
        self.socket.curve_secretkey = self._private_key
        self.socket.curve_publickey = self._public_key
        self.socket.curve_server = True  # must come before bind
        self.socket.bind(f"tcp://*:{self.port}")

        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while self.is_running:
            try:
                events = dict(poller.poll(1000))  # 1 second timeout
                if self.socket in events:
                    message = self.socket.recv_string()
                    print(f"Received: {message}")
                    self.socket.send_string("Message received securely")
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break  # Interrupted
                else:
                    raise

        auth.stop()

    @property
    def public_key(self):
        """
        Get the public key of the IoT device.

        Returns:
            str: The public key as a string in Z85 (ZeroMQ's encoding) format.
        """
        return self._public_key.decode('ascii')


# This part is optional, allowing the script to be run standalone for testing
if __name__ == "__main__":
    iot_device = IoTDeviceManager(5555)
    iot_device.start()

    try:
        input("Press Enter to stop the IoT Device...")
    finally:
        iot_device.stop()