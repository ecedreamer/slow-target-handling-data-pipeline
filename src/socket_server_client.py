import socket
import threading
import time

HOST = "127.0.0.1"
PORT = 7779

def socket_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    conn, address = server.accept()
    print(f"Connection from: {str(address)}")
    count = 0
    data_list = []
    while True:
        data = conn.recv(2048).decode()
        data_list.append(data)
        count += 1
        if not data:
            break
    server.close()
    print("RECEIVED:", len(data_list))



def socket_client():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))
    data = "c" * 2048
    count = 0
    while True:
        count += 1
        p = client.send(data.encode())
        if count % 1000 == 0:
            print(p)
        if count == 10000:
            print
            break
    client.close()
    print("SENT:", count)


def main():
    t1 = threading.Thread(target=socket_server)
    t2 = threading.Thread(target=socket_client)
    t1.start()
    t2.start()
    t1.join()
    t2.join()


if __name__ == '__main__':
    main()