from messages import *
from states import ModelStateMachine, ModelStates
from infrastructure import Scheduler
from abc import ABC, abstractmethod
from collections import deque
from math import ceil
from plotting import plot_trajectory
from numpy import concatenate

class Model():
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
        self.input_models = set()
        self.logger = None

        ## Logging ##
        self.batch_size = 1000
        self.log_buffer = deque()

    def execute(self):
        #print(f'{self.name}: {self.statemachine}')
        match self.get_state():
            case ModelStates.INITIALIZING:
                for model in self.input_models:
                    msg = Event(self.get_address(), model.get_address(), EventActions.DATA_REQUEST, self.get_timestamp()) # fix model get address here
                    self.send(msg)
                self.initialize()
                self.statemachine.trigger('INITIALIZED')

            case ModelStates.PROCESSING: # Input data is valid, model is processing events within [t, t+dt)
                self.process_available_events()
                if self.scheduler.get_next_event_time() is not None:
                    if self.scheduler.get_next_event_time() >= self.get_next_timestamp():
                        self.statemachine.trigger('INCREMENT_TIME') # Transition to evolving

            case ModelStates.EVOLVING: # Model progressing time
                self.evolve()
                self.increment_time()
                if self.logger:
                    self.log()
                self.statemachine.trigger('EVOLVED') # Transition to processing
    
    def evolve(self):
        # Update internal state by dt
        print(f'{self.name}: Evolving from {self.get_timestamp()} to {self.get_next_timestamp()}')

    def increment_time(self):
        self.tick += 1

    def process_available_events(self):
        if self.scheduler.get_next_event_time() is None:
            return
        else:
            if  self.scheduler.get_next_event_time() < self.get_next_timestamp(): 
                event = self.scheduler.pop_next_event()
                self.process_event(event)

    def process_event(self, msg):
        if self.get_next_timestamp() < msg.timestamp:
            raise ValueError(f'{self.name:} Attempting to process message in future')

        match msg.action:
            case EventActions.DATA_PUSH:
                print(f'{self.name} is pulling input from {msg.sender.name} valid at {msg.timestamp}')
                (self.input, validity_end_time) = msg.payload

            case EventActions.DATA_REQUEST: 
                # data will be pushed with a valid time <= this model's timestamp
                sender = msg.sender
                msg = Event(self.get_address(), sender, EventActions.DATA_PUSH, self.get_timestamp(), (self.output, self.get_next_timestamp()))
                self.send(msg)
                msg = Event(self.get_address(), sender, EventActions.DATA_VALID, self.get_next_timestamp())
                self.send(msg)
            
            case EventActions.DATA_VALID:
                msg = Event(self.get_address(), msg.sender, EventActions.DATA_REQUEST, self.get_next_timestamp()) # fix model get address here
                self.send(msg)

            case EventActions.START:
                print(f'{self.name}: Opened Begin message. Starting at {self.get_timestamp()}')
                #self.initialize()
                #self.log()
            
            case EventActions.TERMINATE:
                print(f'{self.name}: End message Received. {self.name} is Done!')
                self.finish()
                if self.logger:
                    self.flush_log()
                    msg = Event(self.get_address(), self.logger.scheduler, EventActions.SIM_COMPLETE, self.get_timestamp())
                    self.send(msg)
                #for pr in self.output_processes:
                #    msg = Event(self, pr, EventActions.SIM_COMPLETE, self.get_timestamp())
                #    self.send(msg)
                msg = Event(self.get_address(), self.controller.scheduler, EventActions.SIM_COMPLETE, self.get_timestamp()) # Fix
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
       p.input_models.add(self)

    def get_timestamp(self):
        return self.tick / self.frequency 
    
    def get_next_timestamp(self):
        return (self.tick + 1) / self.frequency 
