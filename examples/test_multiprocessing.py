# test_multiprocessing.py
# Andrew DeOrio
# 2016-09-19
#
# First, start this program, which starts 5 threads
# $ python3 test_multiprocessing.py
# Then, view the processes and threads running on your system
# $ ps -ax
#
# Reference: https://docs.python.org/3/library/multiprocessing.html

from multiprocessing import Process
import os

def worker(worker_id):
    pid=os.getpid()
    print("Worker {}  pid={}".format(worker_id, pid))
    while True:
        # spin
        pass

if __name__ == "__main__":
    processes=[]
    for i in range(5):
        p = Process(target=worker, args=(i,))
        processes.append(p)
        p.start()
    p.join()
