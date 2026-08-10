from processes import Process
from messaging import ControllerMailbox
from math import ceil

class Logger(Process):
    def __init__(self):
        super().__init__()
        self.mailbox = ControllerMailbox() # This could share code with controller
        self.process = None

        ## Logging contents. Perhaps move to a separate class
        self.index = 0
        self.state_log  = None
        self.output_log = None
        self.time_log = None
    
    def execute(self):
        while self.next_msg:
           self.process_message(self.next_msg)
           self.next_msg = None
           self.check_inbox() # This could share code with controller
    
    def initialize(self):
        # Pre-allocate arrays 
        n = ceil((self.process.tf - self.process.t0) * self.process.frequency)
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
                self.mailbox.remove_process(msg.sender)
                # Terminate code. Need another check here if logging from multiple
                self.finish()
                msg = Message(self, self.controller, Actions.SIM_COMPLETE, self.get_timestamp())
                self.send(msg)

            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')
