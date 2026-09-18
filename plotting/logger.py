from infrastructure.node import Node
from queue import deque
from modeling.model import Model
from infrastructure.messages import *
from math import ceil
from numpy import concatenate
from plotting.plots import plot_trajectory

class Log:
    def __init__(self, n_samples):
        self.max_idx = n_samples
        self.idx = 0
        self.state = [None]*n_samples
        self.output = [None]*n_samples
        self.time = [None]*n_samples

    def append(self, state, output, time):
        if self.idx > self.max_idx:
            raise IndexError(f'Cannot append. Max log elements: {self.max_idx}')
        else:
            self.state[self.idx] = state
            self.output[self.idx] = output
            self.time[self.idx] = time
            self.idx += 1

class Logger(Node):
    def __init__(self):
        super().__init__()
        ## Fix these later
        self.name = 'logger'
        self.t0 = 0
        self.tf = 1
        self.logs = {}
   
    def log(self, data: deque, model):
        while data:
            (state, output, time) = data.popleft()
            self.logs[model].append(state, output, time)

    def process_signal(self, msg):
        match msg.action:
            case SignalActions.LOG:
                #print(f'{self.name} is logging data from {msg.sender.name} valid at {msg.timestamp}')
                self.log(msg.payload, msg.sender)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def listen_to(self, model: Model):
        self.link_to(model.get_address())
        model.logger = self
        n = ceil((model.tf - model.t0) * model.frequency + 1)
        self.logs[model.get_address()] = Log(n)

    def plot_state(self):
        # assuming state is a numpy array...
        for log in self.logs.values():
            state_trajectory = concatenate(log.state, axis=1)
            plot_trajectory(state_trajectory, log.time, 'x')

    def finishing(self):
        pass

    def engaged(self):
        pass

    def disengaged(self):
        pass

    def initialize(self):
        pass

