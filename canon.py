import zmq
from threading import Thread

from machinetalk.protobuf.types_pb2 import *


def worker_thread(context, address, msg_type):
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, bytes(msg_type))
    socket.connect(address)

    while True:
        try:
            payload = socket.recv()  # get data
            print('worker %s received "%s"' % (msg_type, payload))
            # process data
            socket.send('answer %s' % msg_type)  # send response
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break  # Interrupted


def master():
    messages = [MT_EMC_TRAJ_SET_OFFSET,
                MT_EMC_TRAJ_SET_G5X]
    context = zmq.Context().instance()
    context.linger = 0
    threads = []
    address = b'ipc://test'

    socket = context.socket(zmq.ROUTER)
    # let zmq throw an error if a callback is not implemented
    socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
    socket.bind(address)

    # create "callbacks" worker threads
    for msg_type in messages:
        thread = Thread(target=worker_thread,
                        args=(context, address, msg_type))
        thread.start()
        threads.append(thread)

    try:
        socket.send_multipart([bytes(MT_EMC_TRAJ_SET_OFFSET), 'test'])
        print (socket.recv_multipart())
        socket.send_multipart([bytes(MT_EMC_TRAJ_SET_G5X), 'test2'])
        print (socket.recv_multipart())
        socket.send_multipart([bytes(MT_EMC_TRAJ_SET_OFFSET), 'test3'])
        print (socket.recv_multipart())
        socket.send_multipart([bytes(MT_EMC_TRAJ_SET_G5X), 'test4'])
        print (socket.recv_multipart())
        socket.send_multipart([bytes(MT_EMC_TRAJ_SET_G92), 'test5'])  # will throw an error
        print (socket.recv_multipart())
    except KeyboardInterrupt:
        print('Interrupted')
    except zmq.ZMQError as e:
        print (e)

    context.term()


def main():
    master()


if __name__ == "__main__":
    main()
