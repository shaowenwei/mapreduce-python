import collections
import heapq
import os
import shutil
import socket
from threading import *
import time
import json
import re
import string
import sh


class Worker:

    def __init__(self, worker_number, port_number, master_port, master_heartbeat_port):
        self.worker_number=worker_number
        print("worker number: %d" %self.worker_number)
        self.port_number=port_number
        self.master_heartbeat_port=master_heartbeat_port
        self.master_port=master_port

        #create a thread to handle
        do_setup_thread = Thread(target = self.setup_handler)
        do_setup_thread.start()
        
        #create a server socket for worker to listen
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.bind(('127.0.0.1', self.port_number))
        serversocket.listen(20)

        #worker's loop
        data=''
        while 1:
            #listen to the command from master
            (clientsocket, address) = serversocket.accept()
            recv_data = clientsocket.recv(1024).decode("utf-8")
            self.handle_msg(recv_data)


    def handle_msg(self, recv_data):
        data=json.loads(recv_data)
        message=data['message_type']
        out_dir=data['output_directory']
        inf=data['input_files']
        exe=data['executable']
        self.working(inf, out_dir, exe)


    def setup_handler(self):
        #create two threads
        create_heartbeat_thread = Thread(target = self.heartbeat)
        ready_to_work_thread = Thread(target = self.ready_to_work)
        create_heartbeat_thread.start()
        ready_to_work_thread.start()


    def heartbeat(self):
        hb={
            "message_type":"heartbeat",
            "worker_number":self.worker_number
        }
        message=json.dumps(hb)
        while 1:
            sock_hb = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock_hb.sendto(str.encode(message),('127.0.0.1',self.master_heartbeat_port))
            time.sleep(1)


    def ready_to_work(self):
        #when worker is ready to work
        job_dict = {
            "message_type": "status",
            "worker_number": self.worker_number,
            "status": "ready"
        }
        message = json.dumps(job_dict)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", self.master_port))
        sock.sendall(str.encode(message))
        sock.close()


    def working(self, input_files, output_dir,exe):
        #do reducing job
        for file in input_files:
            name=os.path.basename(file)
            with open(file,'r') as f_in:
                run=sh.Command(exe)
                run(_in=f_in, _out=os.path.join(output_dir, name))
            f_in.close()
        #when worker finish his work
        job_dict = {
            "message_type": "status",
            "worker_number": self.worker_number,
            "status": "finished"
        }
        message = json.dumps(job_dict)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", self.master_port))
        sock.sendall(str.encode(message))
        sock.close()
        
        
                        
                    
                
        
