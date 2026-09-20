from abc import ABC, abstractmethod
from enum import Enum, auto
from collections import deque
from math import ceil
from numpy import concatenate
from plotting import *
from infrastructure.util import StateMachine
from infrastructure.node import Actor
from infrastructure.scheduler import Scheduler
from infrastructure.messages import *

class ModelStates(Enum):
    INITIALIZING = auto()
    WAITING      = auto()
    PROCESSING   = auto() 
    EVOLVING     = auto() 
    FINISHING    = auto()

class ModelStateMachine(StateMachine):
    def __init__(self):
        super().__init__()
        self.add_state(ModelStates.INITIALIZING, self.initializing_transition)
        self.add_state(ModelStates.WAITING, self.waiting_transition)
        self.add_state(ModelStates.PROCESSING, self.processing_transition)
        self.add_state(ModelStates.EVOLVING, self.evolving_transition)
        #self.add_state(ModelStates.FINISHING)
        self.set_init_state(ModelStates.INITIALIZING)

    def initializing_transition(self, trig_txt):
        if trig_txt == 'INITIALIZED':
            return ModelStates.WAITING
        else:
            return None

    def waiting_transition(self, trig_txt):
        if trig_txt == 'NEED_INPUTS':
            return ModelStates.WAITING
        elif trig_txt == 'INPUTS_READY':
            return ModelStates.PROCESSING
        else:
            return None

    def processing_transition(self, trig_txt):
        if trig_txt == 'INCREMENT_TIME':
            return ModelStates.EVOLVING
        elif trig_txt == 'NEED_INPUTS':
            return ModelStates.WAITING
        elif trig_txt == 'END_MESSAGE':
            return ModelStates.FINISHING
        else:
            return None

    def evolving_transition(self, trig_txt):
        if trig_txt == 'EVOLVED':
            return ModelStates.PROCESSING
        else:
            return None

class Model(Actor):
    TIME_TOL = 0.001
    def __init__(self):
        self.name = ''
        self.scheduler = Scheduler()
        self.statemachine = ModelStateMachine()

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
        self.input_validity_horizon = {}
        self.logger = None

        ## Logging ##
        self.batch_size = 1000
        self.log_buffer = deque()

    def execute(self):
        self.scheduler.execute()
        #print(f'{self.name}: {self.statemachine}')
        match self.get_state():
            case ModelStates.INITIALIZING:
                self.initialize()
                self.log()
                for model in self.input_validity_horizon.keys():
                    msg = Event(self.get_address(), model.get_address(), EventActions.DATA_REQUEST, self.get_timestamp()) # fix model get address here
                    self.send(msg)
                self.statemachine.trigger('INITIALIZED')

            case ModelStates.WAITING: # Model is blocked because its waiting for inputs
                self.process_available_events()

            case ModelStates.PROCESSING: # Model is processing events within [t, t+dt). All inputs valid
                self.process_available_events()

                # Check if all events are >= t+dt
                if self.scheduler.get_next_event_time() and self.scheduler.get_next_event_time() >= self.get_next_timestamp():
                    self.statemachine.trigger('INCREMENT_TIME') # Transition to evolving

            case ModelStates.EVOLVING: # Model progressing time. Unblocked and all events occur >= t+dt
                print(f'{self.name}: Evolving from {self.get_timestamp()} to {self.get_next_timestamp()}')
                self.evolve()
                self.increment_time()
                if self.logger:
                    self.log()
                self.statemachine.trigger('EVOLVED') # Transition to processing

            case ModelStates.FINISHING:
                # Consume remaining messages at this timestamp
                self.process_available_events()
    
    def evolve(self):
        # Update internal state by dt
        pass

    def increment_time(self):
        self.tick += 1

    def process_available_events(self):
        if self.scheduler.get_next_event_time() is None:
            return
        else:
            if self.scheduler.get_next_event_time() < self.get_next_timestamp(): 
                event = self.scheduler.pop_next_event()
                self.process_event(event)

    def inputs_valid(self):
        if not self.input_validity_horizon:
            return True
        else:
            return all(validity_time >= self.get_timestamp() for validity_time in self.input_validity_horizon.values())

    def process_event(self, msg):
        if self.get_next_timestamp() < msg.timestamp: # Can only process msgs in [t, t+dt)
            raise ValueError(f'{self}: Attempting to process message in future!')

        match msg.action:
            case EventActions.DATA_PUSH:
                print(f'{self.name} is pulling input from {msg.sender.name} valid at {msg.timestamp}')
                (self.input, validity_end_time) = msg.payload
                self.input_validity_horizon[msg.sender] = validity_end_time
                if self.inputs_valid(): 
                    self.statemachine.trigger('INPUTS_READY') # Transition to processing

            case EventActions.DATA_REQUEST: 
                # data will be pushed with a valid time <= this model's timestamp
                sender = msg.sender
                msg = Event(self.get_address(), sender, EventActions.DATA_PUSH, self.get_timestamp(), (self.output, self.get_next_timestamp()))
                self.send(msg)
                msg = Event(self.get_address(), sender, EventActions.DATA_VALID, self.get_next_timestamp())
                self.send(msg)
            
            case EventActions.DATA_VALID:
                if self.get_timestamp() == self.tf:
                    return

                self.statemachine.trigger('NEED_INPUTS') # Transition to waiting
                if msg.timestamp != self.input_validity_horizon[msg.sender.get_address()]:
                    raise ValueError("something in the validity timekeeping is f'ed up")

                if msg.timestamp == self.get_timestamp():
                    request_time = self.get_timestamp()
                else:
                    request_time = self.get_next_timestamp()

                msg = Event(self.get_address(), msg.sender, EventActions.DATA_REQUEST, request_time)
                self.send(msg)

            case EventActions.START:
                print(f'{self.name}: Opened Begin message. Starting at {self.get_timestamp()}')
                #self.initialize()
                #self.log()
            
            case EventActions.TERMINATE:
                print(f'{self}: End message Received. {self} has reached tf')
                self.scheduler.finish()
                self.finish()
                if self.logger:
                    self.flush_log()
                for actor in self.scheduler.mailbox.get_senders():
                    msg = Event(self.get_address(), actor.get_address(), EventActions.SIM_COMPLETE, self.get_timestamp())
                    self.send(msg)
                self.statemachine.trigger('END_MESSAGE')

            case EventActions.SIM_COMPLETE:
                print(f'{self.name}: Notified that {msg.sender.name} is done!')
                self.scheduler.mailbox.disconnect_sender(msg.sender.get_address())
                if self.input_validity_horizon:
                    self.input_validity_horizon.pop(msg.sender.get_address())

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def log(self):
        self.log_buffer.append((self.state, self.output, self.get_timestamp()))
        if len(self.log_buffer) == self.batch_size:
            self.flush_log()

    def flush_log(self):
        msg = Signal(self.get_address(), self.logger.get_address(), SignalActions.LOG, self.log_buffer) # buffer has to be a shallow copy!
        self.log_buffer = deque()
        self.send(msg)

    def link_to(self, model):
        print(f'{self.name} is now linked to {model.name}')
        self.scheduler.link_to(model)

    def get_node(self):
        return self.scheduler.node

    def get_state(self):
        return self.statemachine.get_state()

    def initialize(self):
        print(f'{self.name} is initializing...')

    def finish(self):
        pass

    ## Messaging
    def send(self, msg):
        self.scheduler.send(msg)

    def get_address(self):
        return self.scheduler

     ## Public ## 

    def cascade_into(self, p):
       p.link_to(self) # P will wait for self's message
       self.link_to(p)
       p.input_validity_horizon[self.get_address()] = -1

    def get_timestamp(self):
        return self.tick / self.frequency 
    
    def get_next_timestamp(self):
        return (self.tick + 1) / self.frequency 

