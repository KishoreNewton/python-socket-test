import socket
import socketserver
import threading
import concurrent.futures

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

class CrudHandler(socketserver.BaseRequestHandler):
    data = {}
    data_lock = threading.Lock()

    def handle(self):
        # Set the timeout for the request socket
        self.request.settimeout(300)  # Timeout in seconds (e.g., 300 seconds or 5 minutes)

        while True:
            try:
                client_data = self.request.recv(1024).decode()
                if not client_data:
                    break

                command, key, *value = client_data.split(' ')

                if command == 'CREATE':
                    self.create(key, ' '.join(value))
                elif command == 'READ':
                    self.read(key)
                elif command == 'UPDATE':
                    self.update(key, ' '.join(value))
                elif command == 'DELETE':
                    self.delete(key)
                else:
                    self.request.sendall(b'Invalid command')

            except socket.timeout:
                print("Connection timed out")
                break

    def create(self, key, value):
        with self.data_lock:
            if key in self.data:
                self.request.sendall(b'Key already exists')
            else:
                self.data[key] = value
                self.request.sendall(b'OK')

    def read(self, key):
        with self.data_lock:
            if key in self.data:
                self.request.sendall(self.data[key].encode())
            else:
                self.request.sendall(b'Key not found')

    def update(self, key, value):
        with self.data_lock:
            if key in self.data:
                self.data[key] = value
                self.request.sendall(b'OK')
            else:
                self.request.sendall(b'Key not found')

    def delete(self, key):
        with self.data_lock:
            if key in self.data:
                del self.data[key]
                self.request.sendall(b'OK')
            else:
                self.request.sendall(b'Key not found')

if __name__ == "__main__":
    host, port = 'localhost', 9999

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        server = ThreadedTCPServer((host, port), CrudHandler)
        server.timeout = 300  # Timeout in seconds for the server socket
        print(f'Server started at {host}:{port}')
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            server.shutdown()
            server.server_close()

