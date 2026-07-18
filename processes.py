from collections import deque
from copy import copy
from messages import *

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

    def check_inbox(self, message):
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
        self.inbox[p] = deque()
        p.outbox[self] = deque()

    def get_timestamp(self):
        return 0

    def get_next_timestamp(self):
        return float('inf')

class PhysicalProcess(Process):
    def __init__(self):
        super().__init__()

        ## I/O ##
        self.output = 0
        
        ## Timekeeping ##
        self.t0 = 0
        self.tf = 1
        self.frequency = 10 
        self.minor_tick = 0
        self.major_tick = 0

        ## Communication ## 
        self.output_processes = set() # Set of subscribers to be notified when this process evolves

    def execute(self):
        if  self.next_msg:
            prop_time = self.find_propagation_time()
            self.propagate_to(prop_time)
            
            # if close to next message, read it
            if abs(prop_time - self.next_msg.timestamp) < self.TIME_TOL:
                self.process_message(self.next_msg)
                self.check_inbox()

    def evolve(self):
        # Update internal state by dt
        print(f'{self.name}: Evolving from {self.get_timestamp()} to {self.get_timestamp() + 1/self.frequency}')

    def pull(self, output):
        return

    def increment_time(self):
        self.minor_tick += 1
        if self.minor_tick == self.frequency:
            self.minor_tick = 0
            self.major_tick += 1

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
            for process in self.output_processes:
                if self.get_timestamp() <= process.get_next_timestamp() and \
                    self.get_next_timestamp() > process.get_next_timestamp():
                    msg = OutputMessage(self, process)
                    self.send(msg)

    def process_message(self, msg):
        if self.get_timestamp() < msg.timestamp:
            raise ValueError(f'{self.name:} Attempting to process message in future')
        
        match msg.action:
            case PULL:
                self.pull(msg.output)

            case START:
                print(f'{self.name}: Opened Begin message. Starting at {self.get_timestamp()}')
                self.initialize()
                msg = InitializationComplete(msg.sender, self)    
                self.send(msg)
            
            case TERMINATE:
                print(f'{self.name}: End message Received. {self.name} is Done!')
                self.finish()
                for pr in self.outbox.keys():
                    msg = SimulationComplete(self, pr)
                    self.send(msg)

            case SIM_COMPLETE:
                print(f'{self.receiver.name}: Notified that {self.sender.name} is done!')
                self.mailbox.remove_process(msg.sender)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def initialize(self):
        print(f'{self.name} is initializing...')
        pass 

     ## Public ## 

    def cascade_into(self, p):
       p.link_to(self) # P will wait for self's message
       self.output_processes.add(p) # Self will message P every time it evolves

    def get_timestamp(self):
        return self.minor_tick / self.frequency + self.major_tick
    
    def get_next_timestamp(self):
        return (self.minor_tick + 1) / self.frequency + self.major_tick 

    def get_output(self, timestamp):
        return self.output

class Controller(Process):
    def __init__(self):
        super().__init__(self)
        self.name = 'Controller'
        self.mailbox = ControllerMailbox()
        self.active_processes = set()

    def execute(self):
        while self.next_msg:
           self.process_message(self.next_msg):
           self.check_inbox()
            
        # Check if sim is over
        if not self.active_processes:
            self.finish()
            return

    def process_message(self, msg):
        match msg.action:
            case INIT_COMPLETE:
                msg = Terminate(self.receiver, self)
                self.send(msg)

            case SIM_COMPLETE:
                print(f'{self.name}: Notified that {msg.sender.name} is done!')
                self.mailbox.remove_process(msg.sender)

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
            msg = Begin(self, process)
            self.send(msg)

