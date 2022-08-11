import sys
import time
import zmq
import random
import socket
import threading
import sqlite3
import os

if os.path.exists('src/db/consumer_queue.db'):
    try:
        os.remove('src/db/consumer_queue.db')
        os.remove('src/db/consumer_queue.db-wal')
        os.remove('src/db/consumer_queue.db-shm')
        print("deleted")
    except Exception as e:
        print("Problem deleting. Maybe files are not present.")


HOST = socket.gethostname()
PORT = 7777


def receive_thread():
    db_connection = sqlite3.connect("src/db/consumer_queue.db")
    context = zmq.Context()
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect("tcp://127.0.0.1:5557")
    t1 = time.time()
    count = 0
    batch_data = []
    msg_count = 0
    while True:
        count += 1
        msg_count += 1
        message = consumer_receiver.recv_string()
        batch_data.append((message, ))
        if count % 10000 == 0:
            db_connection.executemany('INSERT INTO data(message)VALUES(?)', batch_data)
            db_connection.commit()
            batch_data = []
            count = 0
            t2 = time.time()
            avg_mtu = 10000 / (t2-t1)
            print("Receiving Thread:", avg_mtu, "mps")
            print("Receiving Thread:", msg_count, "messages")
            t1 = time.time()


def reconnect_target():
    target_socket = socket.socket()  # instantiate
    try:
        target_socket.connect((HOST, PORT))  # connect to the server
        return target_socket
    except Exception as e:
        return None

def send_thread():
    db_connection = sqlite3.connect("src/db/consumer_queue.db")
    db_connection.execute("PRAGMA auto_vacuum")
    target_socket = reconnect_target()
    if not target_socket:
        while True:
            time.sleep(5)
            print("Retrying connection...")
            target_socket = reconnect_target()
            if target_socket:
                print("Connected to the target socket.....")
                break
    count = 0
    msg_count = 0
    t1 = time.time()
    while True:
        cursor = db_connection.execute("SELECT * FROM data ORDER BY id DESC LIMIT 2000")
        id_list = []
        for data in cursor:
            id_list.append((data[0],))
            count += 1
            msg_count += 1
            target_socket.send(data[1].encode())
            if count % 10000 == 0:
                count = 0
                t2 = time.time()
                avg_mtu = 10000 / (t2-t1)
                print("Sending Thread:", avg_mtu, "mps")
                print("Sending Thread:", msg_count, "messages")
                t1 = time.time()
        db_connection.executemany("DELETE FROM data WHERE id=?", id_list)
        db_connection.commit()
        db_connection.execute("PRAGMA journal_size_limit=0")
        db_connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        


def consumer():
    try:
        consumer_id = random.randrange(1,10005)
        print(f"I am consumer #{consumer_id}")
        t1 = threading.Thread(target=receive_thread)
        t2 = threading.Thread(target=send_thread)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except KeyboardInterrupt as e:
        sys.exit()
    

if __name__ == '__main__':
    db_connection = sqlite3.connect("src/db/consumer_queue.db")
    db_connection.execute("PRAGMA journal_mode=WAL")
    db_connection.execute("CREATE TABLE IF NOT EXISTS data(id integer primary key, message varchar)")
    db_connection.close()
    consumer()