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
from modeling.synchronizer import Synchronizer, SyncStates

class ModelStates(Enum):
    INITIALIZING = auto()
    WAITING      = auto()
    SYNCING      = auto()
    PROCESSING   = auto() 
    EVOLVING     = auto() 
    FINISHING    = auto()

class ModelStateMachine(StateMachine):
    def __init__(self):
        super().__init__()
        self.add_state(ModelStates.INITIALIZING, self.initializing_transition)
        self.add_state(ModelStates.WAITING, self.waiting_transition)
        self.add_state(ModelStates.SYNCING, self.syncing_transition)
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
        if trig_txt == 'SYNC_DONE':
            return ModelStates.PROCESSING
        elif trig_txt == 'NEED_SYNC':
            return ModelStates.SYNCING
        else:
            return None

    def syncing_transition(self, trig_txt):
        if trig_txt == 'SYNC_DONE':
            return ModelStates.PROCESSING
        else:
            return None

    def processing_transition(self, trig_txt):
        if trig_txt == 'INCREMENT_TIME':
            return ModelStates.EVOLVING
        elif trig_txt == 'END_MESSAGE':
            return ModelStates.FINISHING
        else:
            return None

    def evolving_transition(self, trig_txt):
        if trig_txt == 'EVOLVED':
            return ModelStates.WAITING
        elif trig_txt == 'END_MESSAGE':
            return ModelStates.FINISHING
        else:
            return None

class Model(Actor):
    TIME_TOL = 0.001
    def __init__(self):
        self.name = ''
        self.synchronizer = Synchronizer()
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

        ## Logging ##
        self.logger = None
        self.batch_size = 1000
        self.log_buffer = deque()

    def execute(self):
        self.scheduler.execute()
        #print(f'{self.name}: {self.statemachine} {self.synchronizer.statemachine}')
        match self.get_state():
            case ModelStates.INITIALIZING:
                self.initialize()
                self.synchronizer.initialize(self.get_timestamp(), self.get_next_timestamp())
                self.log()
                self.request_inputs()
                self.statemachine.trigger('INITIALIZED') # Start model at WAITING

            case ModelStates.WAITING: # Model is blocked because its waiting for inputs
                self.process_available_events()

                match self.synchronizer.get_state():
                    case SyncStates.REQUEST:
                        self.request_inputs()
                    
                    case SyncStates.SYNC:
                        self.send_sync_messages()

                    case SyncStates.PASS:
                        self.statemachine.trigger('SYNC_DONE')

            case ModelStates.PROCESSING: # Model is processing events within [t, t+dt). All inputs valid at t. 
                self.process_available_events()
                
                # if all events occur >= t+dt, it is safe to evolve
                if self.safe_to_evolve():
                    self.statemachine.trigger('INCREMENT_TIME') # Transition to evolving

            case ModelStates.EVOLVING: # Model progressing time. Unblocked, inputs valid, and all events occur >= t+dt
                print(f'{self.name}: Evolving from {self.get_timestamp()} to {self.get_next_timestamp()}')
                self.evolve()
                self.increment_time()
                self.synchronizer.advance_time(self.get_next_timestamp())
                if self.logger:
                    self.log()

                if self.get_timestamp() == self.tf:
                    self.scheduler.finish()
                    self.finish()
                    if self.logger:
                        self.flush_log()
                    for actor in self.scheduler.mailbox.get_senders():
                        msg = Event(self.get_address(), actor.get_address(), EventActions.SIM_COMPLETE, self.get_timestamp())
                        self.send(msg)
                    self.statemachine.trigger('END_MESSAGE')

                else:
                    self.statemachine.trigger('EVOLVED') # Transition back to waiting

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

    def safe_to_evolve(self):
        next_time = self.scheduler.get_next_event_time()
        return (next_time is not None
            and next_time >= self.get_next_timestamp())

    def request_inputs(self):
        for model in self.synchronizer.get_request_list():
            msg = Event(self.get_address(), model.get_address(), EventActions.DATA_REQUEST, self.get_timestamp(), self.get_next_timestamp()) # fix model get address here
            self.send(msg)
        self.synchronizer.request_sent()

    def send_sync_messages(self):
        for model in self.synchronizer.get_sync_list():
            sync_time = self.synchronizer.calc_sync_time(model)
            msg = Event(self.get_address(), model, EventActions.SYNCRONIZE, sync_time)
            self.send(msg)

    def process_event(self, msg):
        if self.get_next_timestamp() < msg.timestamp: # Can only process msgs in [t, t+dt)
            raise ValueError(f'{self}: Attempting to process message in future!')

        match msg.action:
            case EventActions.DATA_PUSH:
                (self.input, sample_end) = msg.payload
                self.synchronizer.set_input_sample(msg.sender, msg.timestamp, sample_end)
                #print(f'{self.name} is pulling input from {msg.sender.name} valid at {msg.timestamp} to {validity_end_time}')

            case EventActions.DATA_REQUEST: 
                # data will be pushed with a valid time <= this model's timestamp
                request_time = msg.payload
                output_model = msg.sender
                data_valid = max(self.get_next_timestamp(), request_time)
                self.synchronizer.set_output_sample(msg.sender, msg.timestamp, data_valid)
                msg = Event(self.get_address(), output_model, EventActions.DATA_PUSH, self.get_timestamp(), (self.output, data_valid))
                self.send(msg)
                print(f'{self.name} is sending input to {msg.sender.name} at {msg.timestamp} valid until {data_valid}')
            
            case EventActions.SYNCRONIZE:
                pass

            case EventActions.START:
                print(f'{self.name}: Opened Begin message. Starting at {self.get_timestamp()}')
                #self.initialize()
                #self.log()
            
            case EventActions.TERMINATE:
                print(f'{self}: End message Received. {self} has reached tf')

            case EventActions.SIM_COMPLETE:
                print(f'{self.name}: Notified that {msg.sender.name} is done!')
                self.scheduler.mailbox.disconnect_sender(msg.sender.get_address())
                if self.input_validity_horizons:
                    self.input_validity_horizons.pop(msg.sender.get_address())

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
       self.synchronizer.set_output_model(p.get_address())
       p.synchronizer.set_input_model(self.get_address())

    def get_timestamp(self):
        return self.tick / self.frequency 
    
    def get_next_timestamp(self):
        return (self.tick + 1) / self.frequency 

