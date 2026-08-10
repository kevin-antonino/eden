from processes import Process
from messaging import ControllerMailbox
from math import ceil

class Logger(Process):
    def __init__(self):
        super().__init__()
        self.channels = None

        ## Logging contents. Perhaps move to a separate class
        self.index = 0
        self.state_log  = None
        self.output_log = None
        self.time_log = None
    
    def initialize(self):
        self.channels = [process for process in self.mailbox.links if process is not self.controller] 
        process = self.channels[0] # Will break for multi channel
        # Pre-allocate arrays 
        n = ceil((process.tf - process.t0) * process.frequency)
        self.state_log = [None] * n
        self.output_log = [None] * n
        self.time_log = [None] * n
   
    def log(self, data: deque):
        while data:
            (state, output, time) = data.popleft()
            self.state_log[self.index] = state
            self.output_log[self.index] = output
            self.time_log[self.index] = time
            self.index += 1
    
    def process_message(self, msg):
        match msg.action:
            case Actions.START:
                print(f'{self.name}: Got start message')
                self.initialize()  

            case Actions.LOG:
                print(f'{self.name} is logging data from {msg.sender.name} valid at {msg.timestamp}')
                self.log(msg.payload)

            case Actions.SIM_COMPLETE:
                print(f'{msg.receiver.name}: Notified that {msg.sender.name} is done!')
                self.mailbox.disconnect_sender(msg.sender)
                self.channels.remove(msg.sender)
                if not self.channels:
                    self.finish()
                    msg = Message(self, self.controller, Actions.SIM_COMPLETE, self.get_timestamp())
                    self.send(msg)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')
