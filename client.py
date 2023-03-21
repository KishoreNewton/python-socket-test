# client.py
import socket

def send_command(command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', 9999))
        s.sendall(command.encode())
        response = s.recv(1024)
        print('Received', repr(response.decode()))

if __name__ == '__main__':
    while True:
        command = input('Enter command: ')
        send_command(command)

