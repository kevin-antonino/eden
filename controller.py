from node import Node, NodeStates,NodeStateMachine
from messages import *

class Controller(Node):
    def __init__(self):
        super().__init__()
        self.name = 'Controller'
        self.active_processes = set()
        self.recovery_queue = PriorityQueue()

    def process_event(self, msg):
        match msg.action:
            case EventActions.SIM_COMPLETE:
                print(f'{self.name}: Notified that {msg.sender.name} is done!')
                self.mailbox.disconnect_sender(msg.sender)
                self.active_processes.remove(msg.sender)
                if not self.active_processes:
                    self.finish()
            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def process_signal(self, msg):
        match msg.action:
            case SignalActions.EARLIEST_EVENT_REQUEST:
                self.recovery_queue.put(msg.payload)
                self.deadlock_recovery()
            
            case _:
                raise ValueError(f'{self.name:} I dont know what to do with this message')

    def engaged(self):
        msg = self.mailbox.pop_event()
        if msg:
            self.process_event(msg)

    def disengaged(self):
        print(f'Deadlock Detected!')
        for process in self.active_processes:
            msg = Signal(self, process, SignalActions.EARLIEST_EVENT_REQUEST)
            self.send(msg)

    def deadlock_recovery(self):
        if self.recovery_queue.qsize() == len(self.active_processes): # not safe for duplicate messages
            _, safe_time, _ = self.recovery_queue.queue[0]
            #while self.recovery_queue:
            timestamp, _, process = self.recovery_queue.get()
            if timestamp <= safe_time:
                msg = Signal(self, process, SignalActions.UNBLOCK)
                self.send(msg)

    ## Public ## 
    def add_to_queue(self, p):
        self.active_processes.add(p)

    def get_queue(self):
        return self.active_processes

    def initialize(self):
        print(f'{self.name}: is initializing...')
        for process in self.active_processes:
            #self.node.add_descendant(process.get_node())
            msg = Event(self, process.get_address(), EventActions.START, process.t0)
            self.send(msg)
            msg = Event(self, process.get_address(), EventActions.TERMINATE, process.tf)
            self.send(msg)

    def finishing(self):
        pass

