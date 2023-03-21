# server.py
import socketserver

class CrudHandler(socketserver.BaseRequestHandler):
    data = {}

    def handle(self):
        while True:
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

    def create(self, key, value):
        if key in self.data:
            self.request.sendall(b'Key already exists')
        else:
            self.data[key] = value
            self.request.sendall(b'OK')

    def read(self, key):
        if key in self.data:
            self.request.sendall(self.data[key].encode())
        else:
            self.request.sendall(b'Key not found')

    def update(self, key, value):
        if key in self.data:
            self.data[key] = value
            self.request.sendall(b'OK')
        else:
            self.request.sendall(b'Key not found')

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            self.request.sendall(b'OK')
        else:
            self.request.sendall(b'Key not found')

if __name__ == "__main__":
    host, port = 'localhost', 9999
    server = socketserver.ThreadingTCPServer((host, port), CrudHandler)
    print(f'Server started at {host}:{port}')
    server.serve_forever()

