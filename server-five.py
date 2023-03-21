import asyncio
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class CrudProtocol:
    data = {}
    data_lock = asyncio.Lock()

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    async def process_request(self, client_data):
        command, key, *value = client_data.split(' ')

        if command == 'CREATE':
            await self.create(key, ' '.join(value))
        elif command == 'READ':
            await self.read(key)
        elif command == 'UPDATE':
            await self.update(key, ' '.join(value))
        elif command == 'DELETE':
            await self.delete(key)
        else:
            self.writer.write(b'Invalid command')

    async def create(self, key, value):
        async with self.data_lock:
            if key in self.data:
                self.writer.write(b'Key already exists')
            else:
                self.data[key] = value
                self.writer.write(b'OK')

    async def read(self, key):
        async with self.data_lock:
            if key in self.data:
                self.writer.write(self.data[key].encode())
            else:
                self.writer.write(b'Key not found')

    async def update(self, key, value):
        async with self.data_lock:
            if key in self.data:
                self.data[key] = value
                self.writer.write(b'OK')
            else:
                self.writer.write(b'Key not found')

    async def delete(self, key):
        async with self.data_lock:
            if key in self.data:
                del self.data[key]
                self.writer.write(b'OK')
            else:
                self.writer.write(b'Key not found')

    async def run(self):
        while True:
            data = await self.reader.read(100)
            message = data.decode()

            if not message:
                break

            await self.process_request(message)

async def handle_client(reader, writer):
    protocol = CrudProtocol(reader, writer)
    await protocol.run()
    writer.close()
    await writer.wait_closed()

async def main(host, port):
    server = await asyncio.start_server(handle_client, host, port)
    logging.info(f'Server started at {host}:{port}')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    host, port = 'localhost', 9999

    try:
        asyncio.run(main(host, port))
    except KeyboardInterrupt:
        logging.info('Server stopped')

