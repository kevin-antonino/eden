from collections import deque
from copy import copy
from messages import *

class Process(ABC):
    TIME_TOL = 0.001
    def __init__(self):
        self.name = ''
        
        ## Communication ## 
        self.inbox: dict[Process, deque[Message]] = {} 
        self.outbox: dict[Process, deque[Message]] = {} 
        self.next_causal_msg = None

        ## Deadlock Detection
        self.blocked = True     # Make an enum?
        self.engaged = False
        self.count = 0  # Number of messages sent without a signal received
        self.parent = None

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    def receive_message(self, message):
        # Error if process is not in list
        self.inbox[message.sender].append(message)
        
        # check inbox for next causal message 
        if not self.next_causal_msg:
            self.next_causal_msg = self.find_next_message()
        
        # Deadlock detection signalling
        if self.engaged:
            print(f'{self.name}: Already engaged. Signalling...')
            message.sender.signal()
        else:
            self.engaged = True
            self.parent = message.sender

    def find_next_message(self):
        next_msg = None
        next_timestamp = float('inf')
        for p in self.inbox.keys():
            if not self.inbox[p]:
                self.blocked = True
                print(f'{self.name} is waiting on {p.name} at {self.get_timestamp()}')
                break
            if self.inbox[p][0].timestamp < next_timestamp:
                next_msg = self.inbox[p][0]
                next_timestamp = next_msg.timestamp
                self.blocked = False
        return next_msg

    def signal(self):
        self.count = 0
        print(f'{self.name}: signal recieved')

    def finish(self):
        for pr in self.outbox.keys():
            msg = SimulationComplete(self, pr)
            self.add_to_outbox(msg)

    def link_to(self, p):
        print(f'{self.name} is now linked to {p.name}')
        self.inbox[p] = deque()
        p.outbox[self] = deque()

    def add_to_outbox(self, msg: Message):
        self.outbox[msg.receiver].append(msg)

    def notify(self):
        #print(f'{self.name} notifying ...')
        for pr in self.outbox.keys():
            while self.outbox[pr]:
                msg = self.outbox[pr].popleft()
                msg.send()

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
        # Check for messages to read
        if not self.next_causal_msg:
            if self.count == 0:
                self.engaged = False
                print(f'{self.name}: Blocked and disengaged. Signalling to parent...')
                self.parent.signal()  
            return

        else:
            prop_time = self.find_propagation_time()
            self.propagate_to(prop_time)
            
            # if close to next message, read it
            if abs(prop_time - self.next_causal_msg.timestamp) < self.TIME_TOL:
                self.next_causal_msg.open()
                self.next_causal_msg = self.find_next_message()
            
            # Notify output systems 
            self.notify()

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
        if self.next_causal_msg:
            prop_time = self.next_causal_msg.timestamp
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
                    self.add_to_outbox(msg)

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
    def __init__(self, processes: set):
        super().__init__()
        self.name = 'Controller'
        for p in processes:
            p.link_to(self)

    def execute(self):
        # Check inbox for init done messages, or done notifications and read them
        processes = copy(list(self.inbox.keys()))
        for process in processes:
            if self.inbox[process]:
                self.inbox[process][0].open() # Special case of check_inbox
        
        # Respond to messages
        self.notify()

        # Check if sim is over
        if not self.outbox.keys():
            self.finish()
            return

        # check for deadlock
        if self.count == float('inf'): 
            # Execute deadlock protocall
            print(f'Deadlock Detected!')
            earliest_time = float('inf')
            earliest_msg = NullMessage()
            for p in self.outbox.keys():
                msg = p.find_next_message() 
                if msg.timestamp < earliest_time:
                    earliest_time = msg.timestamp
                    earliest_msg = msg

            print(f'Earliest message: {earliest_msg.sender.name} to {earliest_msg.receiver.name} at {earliest_msg.timestamp}')
            earliest_msg.receiver.propagate_to(earliest_msg.timestamp)
            self.count += 1 # Controller sending message to process to read a message - come back to this
            
    ## Public ## 

    def get_queue(self):
        sim_queue = copy(list(self.outbox.keys()))
        sim_queue.append(self)
        return sim_queue

    def initialize(self):
        print(f'{self.name}: is initializing...')
        for process in self.outbox.keys():
            msg = Begin(self, process)
            self.add_to_outbox(msg)

        self.notify()

class Simulation():
    def __init__(self, phys_pr: set):
        self.controller = Controller(phys_pr) 
        for p in phys_pr:
            self.controller.link_to(p)

    def start(self):
        self.controller.initialize()

        queue = self.controller.get_queue()
        while len(queue) > 1:
            for process in queue:
                process.execute()

            queue = self.controller.get_queue()

