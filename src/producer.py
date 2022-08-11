import time
import zmq
import random
import string


def message_creator(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

def producer():
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PUSH)
    zmq_socket.bind("tcp://127.0.0.1:5557")
    count_counter = 0
    count = 0
    t1 = time.time()
    while True:
        count += 1
        message = message_creator(100)
        zmq_socket.send_string(message)
        if count % 10000 == 0:
            count_counter += 1
            count = 0
            t2 = time.time()
            avg_mtu = 10000 / (t2-t1)
            print(avg_mtu, "mps")
            t1 = time.time()
            if count_counter == 10:
                break


if __name__ == '__main__':
    producer()