from infrastructure.node import Node
from queue import deque
from modeling.model import Model
from infrastructure.messages import *
from math import ceil
from numpy import concatenate
from plotting.plots import *
import matplotlib.pyplot as plt

class DataLog:
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

class NumericLog(DataLog):
    def __init__(self, n_samples):
        super().__init__(n_samples) # consider pre-allocating size of nd-array
    
    def overlay_state(self, ax, element=[]):
        state_trajectory = concatenate(log.state, axis=1)
        ax = plot_on_ax(axis, self.time, state_trajectory[element], 
            ylabel='', xlabel='Time [s]')
        return ax

    def plot_state(self):
        state_trajectory = concatenate(log.state, axis=1)
        plot_trajectory(state_trajectory, log.time, 'x')
    
    def plot_output(self):
        output_trajectory = concatenate(log.output, axis=1)
        plot_trajectory(output_trajectory, log.time, 'y')

class Logger(Node):
    def __init__(self):
        super().__init__()
        ## Fix these later
        self.name = 'logger'
        self.logs = {}
   
    def process_signal(self, msg):
        match msg.action:
            case SignalActions.LOG:
                #print(f'{self.name} is logging data from {msg.sender.name} valid at {msg.timestamp}')
                self.log(msg.payload, msg.sender)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def log(self, data: deque, model):
        while data:
            (state, output, time) = data.popleft()
            self.logs[model].append(state, output, time)

    def listen_to(self, model: Model):
        self.link_to(model.get_address())
        model.logger = self
        n = ceil((model.tf - model.t0) * model.frequency + 1)
        self.logs[model.get_address()] = NumericLog(n)

    def get_logs(self):
        return self.logs

    def view(self):
        n_plots = 1
        fig, ax = plt.subplots(n_plots, 1) 
        
        for lg in self.logs.values():
            state = concatenate(lg.output, axis=1)
            scatter_on_ax(ax, lg.time, state[0], 
                ylabel='', xlabel='Time [s]')

        fig.patch.set_facecolor('lightgray')
        plt.show()

    def finishing(self):
        pass

    def engaged(self):
        pass

    def disengaged(self):
        pass

    def initialize(self):
        pass

