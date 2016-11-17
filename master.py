import collections
import heapq
import os
import shutil
import socket
from threading import *
import time
import json
import worker
from multiprocessing import *
import datetime

class Master:
        
    def __init__(self, num_workers, port_number):
        self.num_workers=num_workers
        self.port_number=port_number
        self.job_id=-1
        self.job_status="ok" #no job is working now
        self.worker_ready=[] 
        self.worker_working=[]
        self.master_heartbeat_port=port_number-1
        self.work_stages=["complete","map","group","reduce","complete"]
        self.working_stage=self.work_stages[0] #working stage of a certain job
        self.worker_port_number=[]
        self.output_dir=""
        self.process_list=[] #store all the processes of create workers
        self.job_queue=Queue() #store the job unprocessed in the queue
        self.mapper=""
        self.reducer=""
        self.hb_time={}
        self.workers_conditon={} #store worker_number and job details
        self.dead_num=1000
        self.shutdown_flag=0

        #create var folder
        if os.path.exists('var'):
            shutil.rmtree('var')
        os.makedirs('var')
        
        #create a server socket
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.bind(('127.0.0.1', port_number))
        serversocket.listen(20)

        #create a thread to create workers
        do_setup_thread = Thread(target = self.setup_handler)
        do_setup_thread.start()
        
        data=''
        l=[]
        while 1:
            #server's loop
            
            if self.job_status=="ok" and self.working_stage=="complete" and self.job_queue.empty()==False:
                #execute the job in the queue
                print("execute jobs in the queue")
                status=self.handle_msg(self.job_queue.get())
                
            #accept connections from outside and message from workers
            (clientsocket, address) = serversocket.accept()
            recv_data = clientsocket.recv(1024).decode("utf-8")
            data=json.loads(recv_data)
                
            if data['message_type']=="new_master_job":
                #check if the workers are working or no worker is ready
                if self.job_status=="no" or len(self.worker_ready)==0:
                    self.job_queue.put(data)
                    print("put in queue")
                    l.append(data)
                    print(l)
                    
                if self.job_status=="ok" and self.working_stage=="complete" and self.job_queue.empty():
                    print("queue is empty now")
                    status=self.handle_msg(data)
                    
            else:
                status=self.handle_msg(data)


    def handle_msg(self, data):
        message=data['message_type']
        if message=="status":
            print(data['status'])
            #check if workers are ready
            if data['status']=="ready":
                self.worker_ready.append(data['worker_number'])
                print(self.worker_ready)
                
                # if receive the message between working
                if self.working_stage!="complete":
                    old_num=self.dead_num
                    new_num=data['worker_number']
                    
                    if self.working_stage=="map":
                        files=self.workers_conditon[old_num][0]
                        job_id=self.workers_conditon[old_num][1]
                        self.worker_working.append(data['worker_number'])
                        job_dict = {
                            "message_type": "new_worker_job",
                            "input_files": files,
                            "executable": self.mapper,
                            "output_directory": "var/job-%d/mapper-output" %job_id
                        }
                        #save worker mapper job
                        self.workers_conditon[new_num]=[files,job_id]
                        message=json.dumps(job_dict)
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect(( "127.0.0.1",self.worker_port_number[new_num]))
                        sock.sendall(str.encode(message))
                        sock.close()

                    if self.working_stage=="reduce":
                        files=self.workers_conditon[old_num][0]
                        job_id=self.workers_conditon[old_num][1]
                        self.worker_working.append(data['worker_number'])
                        job_dict = {
                            "message_type": "new_worker_job",
                            "input_files": files,
                            "executable": self.reducer,
                            "output_directory": "var/job-%d/reducer-output" %job_id
                        }
                        #save worker reducer job
                        self.workers_conditon[new_num]=[files,job_id]
                        message=json.dumps(job_dict)
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect(( "127.0.0.1",self.worker_port_number[new_num]))
                        sock.sendall(str.encode(message))
                        sock.close()
                    
                
            #check if workers have finished their jobs
            if data['status']=="finished":
                worker_finished=data['worker_number']
                a=0
                for i in self.worker_working:
                    if i==worker_finished:
                        del self.worker_working[a]
                    a=a+1
                print(self.worker_working)
                a=0
                b=0
                
                if len(self.worker_working)==0:
                    for s in self.work_stages:
                        if self.working_stage==s:
                            b=a
                        a=a+1
                    if self.working_stage!="complete":
                        self.working_stage=self.work_stages[b+1]
                        grouper_filenames=[]
                        
                        if self.working_stage=="group":
                            print(self.working_stage)
                            in_dir='var/job-%d/mapper-output' %self.job_id 
                            out_dir='var/job-%d/grouper-output' %self.job_id
                            grouper_filenames=self.__staff_run_group_stage(in_dir, out_dir, len(self.worker_ready))
                            self.working_stage=self.work_stages[3]
                            
                        if self.working_stage=="reduce": 
                            print(self.working_stage)
                            for wo in range(len(self.worker_ready)):
                                self.worker_working.append(self.worker_ready[wo])
                                in_file=[]
                                filenum=len(grouper_filenames)
                                for i in range(filenum):
                                    a = i % len(self.worker_ready)
                                    if a == wo:
                                        in_file.append(grouper_filenames[i])
                                job_dict = {
                                    "message_type": "new_worker_job",
                                    "input_files": in_file,
                                    "executable": self.reducer,
                                    "output_directory": "var/job-%d/reducer-output" %self.job_id
                                }
                                #save worker reducer job
                                self.workers_conditon[self.worker_ready[wo]]=[in_file,self.job_id]

                                message=json.dumps(job_dict)
                                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                sock.connect(( "127.0.0.1",self.worker_port_number[self.worker_ready[wo]]))
                                sock.sendall(str.encode(message))
                                sock.close()
                                
                    if self.working_stage=="complete":
                        output_dir=self.output_dir
                        if os.path.exists(output_dir)==0:
                            os.makedirs(output_dir)
                        input_dir="var/job-%d/reducer-output" %self.job_id
                        for in_filename in os.listdir(input_dir):
                            output_file=os.path.join(output_dir,in_filename)
                            f_out=open(output_file,'w')
                            filename=os.path.join(input_dir,in_filename)
                            with open(filename, 'r') as f_in:
                                for line in f_in:
                                    f_out.write(line)
                            f_in.close()
                            f_out.close()
                        self.job_status="ok" #no job is working
                        print("complete")
                        print(" ")
                        return "complete"
                        
        #create a new job to each worker
        if message=="new_master_job":
            self.reducer=data['reducer_executable']
            self.mapper=data['mapper_executable']
            print("new job")
            self.job_id=self.job_id+1
            self.job_status="no" #there is a processing job
            print(self.job_id)
            if os.path.exists('var/job-%d' %self.job_id):
                shutil.rmtree('var/job-%d' %self.job_id)
            os.makedirs('var/job-%d' %self.job_id)
            os.makedirs('var/job-%d/mapper-output' %self.job_id)
            os.makedirs('var/job-%d/grouper-output' %self.job_id)
            os.makedirs('var/job-%d/reducer-output' %self.job_id)
            self.output_dir=data['output_directory']
            input_dir=data['input_directory']
            message=data['message_type']

            #mapping
            self.working_stage=self.work_stages[1]
            print(self.working_stage)
            
            for wo in range(len(self.worker_ready)):
                self.worker_working.append(self.worker_ready[wo])
                files=[]
                input_files=self.distribute(input_dir,wo,len(self.worker_ready))
                for file in input_files:
                    files.append(file)
                job_dict = {
                    "message_type": "new_worker_job",
                    "input_files": files,
                    "executable": self.mapper,
                    "output_directory": "var/job-%d/mapper-output" %self.job_id
                }
                #save worker mapper job
                self.workers_conditon[self.worker_ready[wo]]=[files,self.job_id]
                message=json.dumps(job_dict)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(( "127.0.0.1",self.worker_port_number[self.worker_ready[wo]]))
                sock.sendall(str.encode(message))
                sock.close()
                
        if message=="shutdown":
            #shutdown the server, eliminate all the worker process
            print("shutdown")
            self.shutdown_flag=1
            for process in self.process_list:
                process.terminate()

            
    def setup_handler(self):
        #create processes for workers
        for i in range(self.num_workers):
            self.worker_port_number.append(self.port_number+i+1)
            process_create_worker=Process(target=self.create_worker, args=(i,))
            self.process_list.append(process_create_worker)
            process_create_worker.start()
 
        #create two threads
        listen_heartbeat_thread = Thread(target = self.heartbeat)
        fault_tol_handler_thread = Thread(target = self.fault_tol_handler)
        listen_heartbeat_thread.start()
        fault_tol_handler_thread.start()

    #create worker
    def create_worker(self,i):
        worker_number=i
        worker_port_number=self.port_number+i+1
        heartbeat_port=self.master_heartbeat_port
        worker_ = worker.Worker(worker_number, worker_port_number, self.port_number, heartbeat_port)


    def heartbeat(self):
        sock_hb = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_hb.bind(('127.0.0.1', self.master_heartbeat_port))
        
        while 1:
            data, address=sock_hb.recvfrom(1024)
            recv_data=data.decode("utf-8")
            data=json.loads(recv_data)
            if data['message_type']=="heartbeat":
                self.hb_time[data['worker_number']]=datetime.datetime.now()

                        
    def fault_tol_handler(self):
        while 1:
            if self.shutdown_flag:
                break
            time.sleep(10)
            curr_time=datetime.datetime.now()
            for i in list(self.hb_time):
                if ((curr_time-self.hb_time[i]).seconds)>10:
                    print("%d is dead" %i)
                    self.worker_ready.remove(i)
                    if i in self.worker_working:
                        self.worker_working.remove(i)
                    self.process_list[i].terminate()
                    del self.hb_time[i]
                    process_create_worker=Process(target=self.create_worker, args=(i,))
                    self.process_list.append(process_create_worker)
                    process_create_worker.start()
                    self.dead_num=i 
                    
        
    def __staff_run_group_stage(self, input_dir, output_dir, num_workers):
        # Loop through input directory and get all the files generated in Map stage
        filenames = []

        for in_filename in os.listdir(input_dir):
            filename=os.path.join(input_dir, in_filename)

            # Open file, sort it now to ease the merging load later
            with open(filename, 'r') as f_in:
                content = sorted(f_in)

            # Write it back into the same file
            with open(filename, 'w+') as f_out:
                f_out.writelines(content)

            # Remember it in our list
            filenames.append(filename)

        # Create a new file to store ALL the sorted tuples in one single
        sorted_output_filename = os.path.join(output_dir, 'sorted.txt')
        sorted_output_file = open(sorted_output_filename, 'w+')

        # Open all files in a single map command! Python is cool like that!
        files = map(open, filenames)

        # Loop through all merged files and write to our single file above
        for line in heapq.merge(*files):
            sorted_output_file.write(line)

        sorted_output_file.close()

        # Create a circular buffer to distribute file among number of workers
        grouper_filenames = []
        grouper_fhs = collections.deque(maxlen=num_workers)

        for i in range(num_workers):
            # Create temp file names
            basename = "file{0:0>4}.out".format(i)
            filename = os.path.join(output_dir, basename)

            # Open files for each worker so we can write to them in the next loop
            grouper_filenames.append(filename)
            fh = open(filename, 'w')
            grouper_fhs.append(fh)

        # Write lines to grouper output files, allocated by key
        prev_key = None
        sorted_output_file = open(os.path.join(output_dir, 'sorted.txt'), 'r')

        for line in sorted_output_file:
            # Parse the line (must be two strings separated by a tab)
            tokens = line.rstrip().split("\t", 2)
            assert len(tokens) == 2, "Error: improperly formatted line"
            key, value = tokens

            # If it's a new key, then rotate circular queue of grouper files
            if prev_key != None and key != prev_key:
                grouper_fhs.rotate(1)

            # Write to grouper file
            fh = grouper_fhs[0]
            fh.write(line)

            # Update most recently seen key
            prev_key = key

        # Close grouper output file handles
        for fh in grouper_fhs:
            fh.close()

        # Delete the sorted output file
        sorted_output_file.close()
        os.remove(sorted_output_filename)

        # Return array of file names generated by grouper stage
        return grouper_filenames

    # distribute files among ready workers 
    def distribute(self, input_dir,worker,num_workers):
        in_file=[]
        filenames = []
        filenum=0
        for in_filename in os.listdir(input_dir):
            filename=os.path.join(input_dir,in_filename)
            filenames.append(filename)
        filenum=len(filenames)
        for i in range(filenum):
            a = i % num_workers
            if a==worker:
                in_file.append(filenames[i])
        return in_file
        
