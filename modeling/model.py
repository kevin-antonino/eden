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
    SYNCING         = auto()
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
        if trig_txt == 'READY':
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

        ## Syncronization ## 
        self.input_validity_horizon = {}
        self.output_validity_horizon = {}
        self.sync_times = {}

        ## Logging ##
        self.logger = None
        self.batch_size = 1000
        self.log_buffer = deque()

    def execute(self):
        self.scheduler.execute()
        #print(f'{self.name}: {self.statemachine}')
        match self.get_state():
            case ModelStates.INITIALIZING:
                self.initialize()
                self.log()
                self.request_inputs()
                self.statemachine.trigger('INITIALIZED') # Start model at WAITING

            case ModelStates.WAITING: # Model is blocked because its waiting for inputs
                self.process_available_events()

                if self.inputs_valid():
                    if self.need_to_sync():
                        print(f'{self.name}: Transitioning to SYNC')
                        self.statemachine.trigger('NEED_SYNC')
                    else:
                        self.statemachine.trigger('READY') # Transition to processing. Inputs valid at t

            case ModelStates.SYNCING:
                self.process_available_events()

                if self.outputs_valid():
                    self.send_sync_messages()
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
                    self.request_inputs()
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

    def inputs_valid(self):
        if not self.input_validity_horizon:
            return True
        else:
            return all([valid_end_time >= self.get_next_timestamp() for valid_end_time in self.input_validity_horizon.values()])
    
    def outputs_valid(self):
        if not self.output_validity_horizon:
            return True
        else:
            return all([valid_end_time >= self.get_next_timestamp() for valid_end_time in self.output_validity_horizon.values()])

    def need_to_sync(self):
        if not self.sync_times:
            return False
        else:
            return any([time < self.get_next_timestamp() for time in self.sync_times.values()])

    def safe_to_evolve(self):
        if self.scheduler.get_next_event_time() and self.scheduler.get_next_event_time() >= self.get_next_timestamp():
            return True
        else:
            return False

    def request_inputs(self):
        for model in self.input_validity_horizon.keys():
            if self.input_validity_horizon[model] <= self.get_next_timestamp():
                msg = Event(self.get_address(), model.get_address(), EventActions.DATA_REQUEST, self.get_timestamp(), self.get_next_timestamp()) # fix model get address here
                self.send(msg)

    def send_sync_messages(self):
        for model in self.output_validity_horizon.keys():
            self.sync_times[model] = min(self.output_validity_horizon[model], self.input_validity_horizon[model])
            msg = Event(self.get_address(), model, EventActions.SYNCRONIZE, self.sync_times[model])
            self.send(msg)

    def process_event(self, msg):
        if self.get_next_timestamp() < msg.timestamp: # Can only process msgs in [t, t+dt)
            raise ValueError(f'{self}: Attempting to process message in future!')

        match msg.action:
            case EventActions.DATA_PUSH:
                (self.input, sample_end) = msg.payload
                validity_end_time = max(sample_end, self.get_next_timestamp())
                self.input_validity_horizon[msg.sender] = validity_end_time
                print(f'{self.name} is pulling input from {msg.sender.name} valid at {msg.timestamp} to {validity_end_time}')

            case EventActions.DATA_REQUEST: 
                # data will be pushed with a valid time <= this model's timestamp
                request_time = msg.payload
                output_model = msg.sender
                data_valid = max(self.get_next_timestamp(), request_time)
                self.output_validity_horizon[output_model] = data_valid
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
       p.input_validity_horizon[self.get_address()] = -float('inf') 
       self.sync_times[p.get_address()] = 0

    def get_timestamp(self):
        return self.tick / self.frequency 
    
    def get_next_timestamp(self):
        return (self.tick + 1) / self.frequency 

