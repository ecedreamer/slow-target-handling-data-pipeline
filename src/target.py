import socket
import time


HOST = socket.gethostname()
PORT = 7777

def server_program():
    server_socket = socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen(2)
    conn, address = server_socket.accept()
    print(f"Connection from: {str(address)}")
    count = 0
    t1 = time.time()
    loop_count = 1000
    msg_count = 0

    while True:
        data = conn.recv(1024).decode()
        count += 1
        msg_count += 1
        if count % loop_count == 0:
            count = 0
            t2 = time.time()
            avg_mtu = loop_count / (t2-t1)
            if t2-t1 > 5:
                loop_count /= 5
            elif t2-t1 > 2:
                loop_count /= 2
            elif t2-t1 < 1:
                loop_count = 10000
            print(avg_mtu, "mps")
            print(msg_count, "messages")
            t1 = time.time()
        if not data:
            break
    print("Received all ", msg_count, "messages")
    server_socket.close()
    


if __name__ == '__main__':
    server_program()