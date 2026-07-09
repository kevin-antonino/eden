from collections import deque
from dataclasses import dataclass
from copy import copy

class Process():
    def __init__(self):
        self.name = ''

        ## I/O ##
        self.output = 0

        ## Status ##
        self.blocked = True
        self.complete = False # do away with this
        
        ## Timekeeping ##
        self.t0 = 0
        self.tf = 1
        self.frequency = 10 
        self.minor_tick = 0
        self.major_tick = 0

        ## Communication ## 
        self.inbox = {}       # Process : Message Queue pairs that this process will wait for
        self.outbox = {}      # Process : Message Queue pairs that will wait for this process
        self.output_processes = set() # Set of subscribers to be notified when this process evolves

        ## Deadlock Detection
        self.engaged = False
        self.count = 0  # Number of messages sent without a signal received
        self.parent = None

    def execute(self):
        # Get Earliest Message 
        msg = self.check_inbox()

        if not self.blocked: 
            # Update system so msg is in [ts, ts+L]
            prop_time = self.get_propagation_time()
            self.propagate_to(prop_time)

            if prop_time == msg.timestamp:
                # perform action now that it's between [ts, ts+L]
                msg.open()

                # Respond to any messages
                self.notify()

    def get_propagation_time(self, msg):
        prop_time = msg.timestamp
        for p in self.output_processes:
            if p.get_next_timestamp() < prop_time:
                prop_time = p.get_next_timestamp()

        return prop_time

    def propagate_to(self, t):
        while self.get_timestamp() < t:
            self.evolve()
            self.increment_time()
            for process in self.output_processes:
                if self.get_timestamp() <= process.get_timestamp() and self.get_next_timestamp() > process.get_timestamp()
                    OutputMessage(self, process)

            self.notify()

    def check_inbox(self): 
        min_msg_time = float('inf')
        earliest_msg = NullMessage()
        self.blocked = False
        for process in self.inbox.keys():
            if not self.inbox[process]: 
                self.blocked = True
                print(f'{self.name} is waiting on {process.name} at {self.get_timestamp()}')
                if self.count == 0:
                    self.engaged = False
                    print(f'{self.name}: Blocked and disengaged. Signalling to parent...')
                    self.parent.signal()  
                break

            else:
                first_msg = self.inbox[process][0]
                if first_msg.timestamp < min_msg_time:
                    min_msg_time = first_msg.timestamp
                    earliest_msg = first_msg

        return earliest_msg

    def add_to_outbox(self, msg: Message):
        self.outbox[msg.receiver].append(msg)

    def evolve(self):
        # Update internal state by dt
        print(f'{self.name}: Evolving from {self.get_timestamp()} to {self.get_timestamp() + 1/self.frequency}')

    def notify(self):
        #print(f'{self.name} notifying ...')
        for pr in self.outbox.keys():
            while self.outbox[pr]:
                msg = self.outbox[pr].popleft()
                msg.send()

    def pull(self, output):
        return

    def increment_time(self):
        self.minor_tick += 1
        if self.minor_tick == self.frequency:
            self.minor_tick = 0
            self.major_tick += 1

    ## Protected ##

    def receive_message(self, message):
        # Error if process is not in list
        self.inbox[message.sender].append(message)
        if self.engaged:
            print(f'{self.name}: Already engaged. Signalling...')
            message.sender.signal()
        else:
            self.engaged = True
            self.parent = message.sender

    def signal(self):
        self.count = 0
        print(f'{self.name}: signal recieved')

    def finish(self):
        self.complete = True
        for pr in self.outbox.keys():
            SimulationComplete(self, pr)

    def link_to(self, p):
        print(f'{self.name} is now linked to {p.name}')
        self.inbox[p] = deque()
        p.outbox[self] = deque()

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
        if self.count == 0: 
            # Execute deadlock protocall
            print(f'Deadlock Detected!')
            earliest_time = float('inf')
            earliest_msg = NullMessage()
            for p in self.outbox.keys():
                msg = p.check_inbox() 
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
            Begin(self, process)

        self.notify()

class Simulation():
    def __init__(self, phys_pr: set):
        self.controller = Controller(phys_pr) 
        for p in phys_pr:
            self.controller.link_to(p)

    def start(self):
        self.controller.initialize()

        # Needs a queue that gets smaller. if just controller, end the sim.    
        while not self.controller.complete:
            queue = self.controller.get_queue()
            for process in queue:
                process.execute()

