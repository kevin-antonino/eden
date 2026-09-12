from messages import *
from scheduling import *
from abc import ABC, abstractmethod
from collections import deque
from math import ceil
from plotting import plot_trajectory
from numpy import concatenate

class Process(ABC):
    TIME_TOL = 0.001
    def __init__(self):
        self.name = ''

    def __str__(self):
        return self.name

    @abstractmethod
    def process_event(self, msg):
        ...

    def send(self, msg):
        self.scheduler.send(msg)

    def link_to(self, p):
        print(f'{self.name} is now linked to {p.name}')
        self.scheduler.mailbox.connect_sender(p)

    def get_node(self):
        return self.scheduler.node

    def initialize(self):
        print(f'{self.name} is initializing...')

    def finish(self):
        pass

class PhysicalProcess(Process):
    def __init__(self):
        super().__init__()
        self.scheduler = ConservativeScheduler()
        #self.state_machine = StateMachine()

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
        self.input_pulled = {}
        self.controller = None
        self.logger = None

        ## Logging ##
        self.batch_size = 1000
        self.log_buffer = deque()

    '''
    def execute(self):

        match self.state_machine.state:
            case States.WAITING:
                self.handle_events()
                if all(self.input_pulled):
                    self.state_machine.inputs_pulled()
                    self.input_pulled = {process : False for process in self.input_pulled.keys()}

            case States.EVOLVING:
                self.evolve()
                self.increment_time()
                if self.logger:
                    self.log()
                self.state_machine.process_evolved()
        '''

    def propagate_to(self, time):
        while self.get_next_timestamp() < time:
            self.evolve()
            self.increment_time()
            if self.logger:
                self.log()

    def evolve(self):
        # Update internal state by dt
        print(f'{self.name}: Evolving from {self.get_timestamp()} to {self.get_next_timestamp()}')

    def increment_time(self):
        self.tick += 1

    def request_inputs(self):
        for process in self.input_pulled.keys():
            msg = Event(self.scheduler, process.scheduler, EventActions.DATA_REQUEST, self.get_next_timestamp())
            self.send(msg)

    def process_event(self, msg):
        if self.get_next_timestamp() < msg.timestamp:
            raise ValueError(f'{self.name:} Attempting to process message in future')

        match msg.action:
            case EventActions.DATA_PUSH:
                print(f'{self.name} is pulling input from {msg.sender.name} valid at {msg.timestamp}')
                (ts, next_ts, self.input) = msg.payload
                self.inputs_pulled[msg.sender] = True
                msg = Event(self.scheduler, msg.sender.scheduler, EventActions.DATA_REQUEST, max(self.get_next_timestamp(), next_ts))
                self.send(msg)

            case EventActions.DATA_REQUEST:
                #print(f'{self.name} is sending input to {msg.sender.name} valid at {self.get_timestamp()}')
                msg = Event(self.scheduler, msg.sender.scheduler, EventActions.DATA_PUSH, self.get_timestamp(), (self.get_timestamp(), self.get_next_timestamp(), self.output))
                self.send(msg)

            case EventActions.START:
                print(f'{self.name}: Opened Begin message. Starting at {self.get_timestamp()}')
                self.initialize()
                self.request_inputs()
                self.log()
            
            case EventActions.TERMINATE:
                print(f'{self.name}: End message Received. {self.name} is Done!')
                self.finish()
                if self.logger:
                    self.flush_log()
                    msg = Event(self.scheduler, self.logger.scheduler, EventActions.SIM_COMPLETE, self.get_timestamp())
                    self.send(msg)
                #for pr in self.output_processes:
                #    msg = Event(self, pr, EventActions.SIM_COMPLETE, self.get_timestamp())
                #    self.send(msg)
                msg = Event(self.scheduler, self.controller.scheduler, EventActions.SIM_COMPLETE, self.get_timestamp())
                self.send(msg)

            case EventActions.SIM_COMPLETE:
                print(f'{self.name}: Notified that {msg.sender.name} is done!')
                self.mailbox.disconnect_sender(msg.sender.scheduler)
                self.input_pulled.remove(msg.sender)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def log(self):
        self.log_buffer.append((self.state, self.output, self.get_timestamp()))
        if len(self.log_buffer) == self.batch_size:
            self.flush_log()

    def flush_log(self):
        msg = Signal(self, self.logger, SignalActions.LOG, self.log_buffer) # buffer has to be a shallow copy!
        self.log_buffer = deque()
        self.send(msg)

     ## Public ## 

    def cascade_into(self, p):
       p.link_to(self) # P will wait for self's message
       p.input_pulled[self] = False
       self.link_to(p)
       #self.output_processes.add(p)

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
        self.active_processes = set()
        self.recovery_queue = PriorityQueue()
        self.mailbox = Mailbox()

    def process_event(self, msg):
        match msg.action:
            case EventActions.SIM_COMPLETE:
                print(f'{self.name}: Notified that {msg.sender.name} is done!')
                self.mailbox.disconnect_sender(msg.sender)
                self.active_processes.remove(msg.sender)
                if not self.active_processes:
                    self.finish()
            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def process_signal(self, msg):
        match msg.action:
            case SignalActions.NEW_NODE:
                print(f'{self.name}: Adding {msg.payload.get_process().name} as a descendent')
                self.node.add_descendant(msg.payload)

            case SignalActions.KILL_NODE:
                print(f'{self.name}: Removing {msg.payload.get_process().name} as a descendent')
                self.node.remove_descendant(msg.payload)
                if not self.node.get_descendants():
                    self.deadlock_detected()

            case SignalActions.EARLIEST_EVENT_REQUEST:
                self.recovery_queue.put(msg.payload)
                self.deadlock_recovery()
            
            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def deadlock_detected(self):
        print(f'Deadlock Detected!')
        for process in self.active_processes:
            msg = Signal(self, process, SignalActions.EARLIEST_EVENT_REQUEST)
            self.send(msg)

    def deadlock_recovery(self):
        if self.recovery_queue.qsize() == len(self.active_processes): # not safe for duplicate messages
            _, safe_time, _ = self.recovery_queue.queue[0]
            #while self.recovery_queue:
            timestamp, _, process = self.recovery_queue.get()
            if timestamp <= safe_time:
                msg = Signal(self, process, SignalActions.UNBLOCK)
                self.send(msg)

    ## Public ## 
    def add_to_queue(self, p):
        self.active_processes.add(p)

    def get_queue(self):
        return self.active_processes

    def initialize(self):
        print(f'{self.name}: is initializing...')
        for process in self.active_processes:
            #self.node.add_descendant(process.get_node())
            msg = Event(self, process, EventActions.START, process.t0)
            self.send(msg)
            msg = Event(self, process, EventActions.TERMINATE, process.tf)
            self.send(msg)
            msg = Signal(self, process, SignalActions.UNBLOCK)
            self.send(msg)

class Logger(Process):
    def __init__(self):
        super().__init__()
        ## Fix these later
        self.channels = None
        self.name = 'logger'
        self.t0 = 0

        ## Logging contents. Perhaps move to a separate class
        self.index = 0
        self.state_log  = None
        self.output_log = None
        self.time_log = None
    
    def initialize(self):
        self.channels = [process for process in self.mailbox.get_senders() if process is not self.controller] 
        process = self.channels[0] # Will break for multi channel
        # Pre-allocate arrays 
        n = ceil((process.tf - process.t0) * process.frequency + 1)
        self.state_log = [None]*n
        self.output_log = [None]*n
        self.time_log = [None]*n
   
    def log(self, data: deque):
        while data:
            (state, output, time) = data.popleft()
            self.state_log[self.index] = state
            self.output_log[self.index] = output
            self.time_log[self.index] = time
            self.index += 1
    
    def process_event(self, msg):
        match msg.action:
            case EventActions.START:
                print(f'{self.name}: Got start message')
                self.initialize()  

            case EventActions.SIM_COMPLETE:
                print(f'{msg.receiver.name}: Notified that {msg.sender.name} is done!')
                self.mailbox.disconnect_sender(msg.sender)
                self.channels.remove(msg.sender)
                if not self.channels:
                    self.finish()
                    msg = Event(self, self.controller, Actions.SIM_COMPLETE)
                    self.send(msg)

    def process_signal(self, msg):
        match msg.action:
            case Actions.LOG:
                print(f'{self.name} is logging data from {msg.sender.name} valid at {msg.timestamp}')
                self.log(msg.payload)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def listen_to(self, p: PhysicalProcess):
        self.link_to(p)
        p.logger = self

    def plot_state(self):
        # assuming state is a numpy array...
        state_trajectory = concatenate(self.state_log, axis=1)
        plot_trajectory(state_trajectory, self.time_log, 'x')

