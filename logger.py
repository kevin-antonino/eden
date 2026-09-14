
class Logger(Node):
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
