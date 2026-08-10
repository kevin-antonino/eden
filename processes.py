from collections import deque
from abc import ABC, abstractmethod
from copy import copy
from messaging import *

class Process(ABC):
    TIME_TOL = 0.001
    def __init__(self):
        self.name = ''
        
        ## Communication ## 
        self.mailbox = Mailbox()
        self.next_msg = None

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def process_message(self, msg):
        pass

    def check_inbox(self):
        if not self.next_msg:
            self.next_msg = self.mailbox.get_next_message()

    def signal(self):
        self.count = 0
        print(f'{self.name}: signal recieved')

    def send(self, msg):
        self.mailbox.add_to_outbox(msg)

    def finish(self):
        pass

    def initialize(self):
        pass

    def link_to(self, p):
        print(f'{self.name} is now linked to {p.name}')
        self.mailbox.add_sender(p)

    def get_timestamp(self):
        return 0

    def get_next_timestamp(self):
        return float('inf')

class PhysicalProcess(Process):
    def __init__(self):
        super().__init__()

        ## State & I/O ##
        self.state  = None
        self.input  = None
        self.output = None
        
        ## Timekeeping ##
        self.t0 = 0
        self.tf = 1
        self.frequency = 10 
        self.tick = 0

        ## Communication ## 
        self.output_processes = set() # Set of subscribers to be notified when this process evolves
        self.controller = None

        ## Logging ##
        self.logger = None
        self.batch_size = 1000
        self.log_buffer = deque()

    def execute(self):
        if  self.next_msg:
            prop_time = self.find_propagation_time()
            self.propagate_to(prop_time)
            
            # if close to next message, read it
            if abs(prop_time - self.next_msg.timestamp) < self.TIME_TOL:
                self.process_message(self.next_msg)
                self.next_msg = None
                self.check_inbox()
        else:
            #print(f'{self.name} is stuck')
            ...

    def evolve(self):
        # Update internal state by dt
        print(f'{self.name}: Evolving from {self.get_timestamp()} to {self.get_timestamp() + 1/self.frequency}')

    def pull(self, output):
        return

    def increment_time(self):
        self.tick += 1

    def find_propagation_time(self):
        prop_time = self.get_timestamp()
        if self.next_msg:
            prop_time = self.next_msg.timestamp
            for p in self.output_processes: 
                if p.get_next_timestamp() < prop_time:
                    prop_time = p.get_next_timestamp()

        return prop_time

    def propagate_to(self, prop_time):
        while self.get_timestamp() < prop_time:
            self.evolve()
            self.increment_time()
            if self.logger:
                self.log()
            self.notify_output_processes()

    def notify_output_processes(self):
        for process in self.output_processes:
            # Primary causality constraint 
            if self.get_timestamp() - process.get_next_timestamp() < self.TIME_TOL:
                # Only send messages when you need to
                if self.get_next_timestamp() - process.get_next_timestamp() > self.TIME_TOL:
                    msg = Message(self, process, Actions.PULL_OUTPUT, self.get_timestamp(), self.output)
                    self.send(msg)

    def process_message(self, msg):
        if self.get_timestamp() < msg.timestamp:
            raise ValueError(f'{self.name:} Attempting to process message in future')
        
        match msg.action:
            case Actions.PULL_OUTPUT:
                print(f'{self.name} is pulling input from {msg.sender.name} valid at {msg.timestamp}')
                self.input = msg.payload

            case Actions.START:
                print(f'{self.name}: Opened Begin message. Starting at {self.get_timestamp()}')
                self.initialize()
                # Tell Controller init is done
                msg = Message(self, self.controller, Actions.INIT_COMPLETE, self.get_timestamp())
                self.send(msg)
            
            case Actions.TERMINATE:
                print(f'{self.name}: End message Received. {self.name} is Done!')
                self.finish()
                if self.logger:
                    self.flush_log()
                msg = Message(self, self.controller, Actions.SIM_COMPLETE, self.get_timestamp())
                self.send(msg)
                for pr in self.output_processes:
                    msg = Message(self, pr, Actions.SIM_COMPLETE, self.get_timestamp())
                    self.send(msg)

            case Actions.SIM_COMPLETE:
                print(f'{msg.receiver.name}: Notified that {msg.sender.name} is done!')
                self.mailbox.disconnect_sender(msg.sender)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def initialize(self):
        print(f'{self.name} is initializing...')
        pass 
    
    def log(self):
        self.log_buffer.append((self.state, self.output, self.get_timestamp()))
        if len(self.log_buffer) == self.batch_size:
            self.flush_log()

    def flush_log(self):
        msg = Message(self, self.logger, Actions.LOG, self.get_timestamp(), self.log_buffer) # buffer has to be a shallow copy!
        self.log_buffer = deque()
        self.send(msg)

     ## Public ## 

    def cascade_into(self, p):
       p.link_to(self) # P will wait for self's message
       self.output_processes.add(p) # Self will message P every time it evolves

    def get_timestamp(self):
        return self.tick / self.frequency 
    
    def get_next_timestamp(self):
        return (self.tick + 1) / self.frequency 

    def get_output(self, timestamp):
        return self.output

class Controller(Process):
    def __init__(self):
        super().__init__()
        self.name = 'Controller'
        self.mailbox = ControllerMailbox()
        self.active_processes = set()

    def execute(self):
        while self.next_msg:
           self.process_message(self.next_msg)
           self.next_msg = None
           self.check_inbox()
            
        # Check if sim is over
        if not self.active_processes:
            self.finish()
            return

    def process_message(self, msg):
        match msg.action:
            case Actions.INIT_COMPLETE:
                tf = msg.sender.tf
                msg = Message(self, msg.sender, Actions.TERMINATE, msg.sender.tf)
                self.send(msg)

            case Actions.SIM_COMPLETE:
                print(f'{self.name}: Notified that {msg.sender.name} is done!')
                self.mailbox.disconnect_sender(msg.sender)
                self.active_processes.remove(msg.sender)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    ## Public ## 
    def add_to_queue(self, p):
        self.active_processes.add(p)

    def get_queue(self):
        return self.active_processes

    def initialize(self):
        print(f'{self.name}: is initializing...')
        for process in self.active_processes:
            msg = Message(self, process, Actions.START, process.t0)
            self.send(msg)
