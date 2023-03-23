import asyncio
import logging
import concurrent.futures

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class CrudProtocol:
    data = {}
    data_lock = asyncio.Lock()
    cpu_executor = concurrent.futures.ThreadPoolExecutor()

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
                # Offload CPU-intensive computation to a separate thread pool
                result = await asyncio.get_running_loop().run_in_executor(
                    self.cpu_executor, self._create_cpu_intensive_task, key, value)
                if result:
                    self.data[key] = value
                    self.writer.write(b'OK')
                else:
                    self.writer.write(b'Error during computation')

    async def read(self, key):
        async with self.data_lock:
            if key in self.data:
                self.writer.write(self.data[key].encode())
            else:
                self.writer.write(b'Key not found')

    async def update(self, key, value):
        async with self.data_lock:
            if key in self.data:
                # Offload CPU-intensive computation to a separate thread pool
                result = await asyncio.get_running_loop().run_in_executor(
                    self.cpu_executor, self._update_cpu_intensive_task, key, value)
                if result:
                    self.data[key] = value
                    self.writer.write(b'OK')
                else:
                    self.writer.write(b'Error during computation')
            else:
                self.writer.write(b'Key not found')

    async def delete(self, key):
        async with self.data_lock:
            if key in self.data:
                # Offload CPU-intensive computation to a separate thread pool
                result = await asyncio.get_running_loop().run_in_executor(
                    self.cpu_executor, self._delete_cpu_intensive_task, key)
                if result:
                    del self.data[key]
                    self.writer.write(b'OK')
                else:
                    self.writer.write(b'Error during computation')
            else:
                self.writer.write(b'Key not found')

    def _create_cpu_intensive_task(self, key, value):
        # Perform a CPU-intensive computation for the CREATE command
        return True  # Return the result of the computation

    def _update_cpu_intensive_task(self, key, value):
        # Perform a CPU-intensive computation for the UPDATE command
        return True  # Return the result of the computation

    def _delete_cpu_intensive_task(self, key):
        # Perform a CPU-intensive computation for the DELETE command
        return True  # Return the result of the computation


    async def run(self):
        while True:
            data = await self.reader.read(100)
            message = data.decode()

            if not message:
                break

            try:
                await self.process_request(message)
            except Exception as e:
                logging.error(f'Error processing request: {e}')

        self.writer.close()
        await self.writer.wait_closed()

async def handle_client(reader, writer):
    protocol = CrudProtocol(reader, writer)
    await protocol.run()

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

