from node import Node

class Scheduler(Node):
    def __init__(self):
        super().__init__()
        self.scheduled_event = None
        self.bypass: bool = False     

    def engaged(self):
        if not self.scheduled_event:
            self.schedule()

    def disengaged(self):
        ...

    def schedule(self):
        if self.safe_event(self.mailbox.get_inbox()) or self.bypass:
            self.schedule_event()
        else:
            self.check_if_blocked()

    def flush_signals(self):
        next_signal = self.mailbox.pop_signal()
        while next_signal:
            self.process_signal(next_signal)
            next_signal = self.mailbox.pop_signal()

    def schedule_event(self):
        self.scheduled_event = self.mailbox.pop_event()
        #if not self.node.in_tree():
            #self.grow_tree(self.scheduled_event)
        if self.bypass:
            self.bypass = False # reset flag if used

    def pop_next_event(self):
        next_event = self.scheduled_event
        if self.scheduled_event:
            self.scheduled_event = None # reset
        return next_event
    
    def get_next_event_time(self):
        if self.scheduled_event:
            return self.scheduled_event.timestamp
        else:
            return None

    def process_signal(self, msg):
        match msg.action:
            case SignalActions.UNBLOCK:
                print(f'{self.name} Unblocking....')
                self.bypass = True

            case SignalActions.EARLIEST_EVENT_REQUEST: # Physical process only
                timestamp = self.mailbox.get_next_event_time()
                dt = 1/self.frequency
                msg = Signal(self, self.controller, SignalActions.EARLIEST_EVENT_REQUEST, (timestamp, timestamp + dt, self))
                self.send(msg)
    
    def safe_event(self, inbox):
        if all(inbox.values()): # If there is a message waiting from all LPs
            return True
        else:
            return False

    def finishing(self):
        self.schedule_event()

