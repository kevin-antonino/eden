from abc import ABC
from states import SimulationStates, SimulationStateMachine
from scheduling import TreeNode
from messages import *

class Actor(ABC):
    def __init__(self):
        self.name = ''

    def __str__(self):
        return self.name

    # get_state can be here
    @abstractmethod
    def execute(self) -> None:
        ...

    @abstractmethod
    def get_address(self):
        ...

class Immaterial(ABC):
    def __init__(self):
        self.name = ''
        self.statemachine = SimulationStateMachine()
        self.mailbox = Mailbox()
        self.node = TreeNode(self)
    
    def execute(self):
        match self.get_state():
            case SimulationStates.ENGAGED:
                self.flush_signals()
                self.engaged()

            case SimulationStates.DISENGAGED:
                self.flush_signals()
                self.disengaged()

    def flush_signals(self):
        next_signal = self.mailbox.pop_signal()
        while next_signal:
            self.handle_signal(next_signal)
            next_signal = self.mailbox.pop_signal()

    def handle_signal(self, msg):
        match msg.action:
            case SignalActions.NEW_NODE:
                print(f'{self.name}: Adding {msg.payload.get_process().name} as a descendent')
                self.node.add_descendant(msg.payload)

            case SignalActions.KILL_NODE:
                print(f'{self.name}: Removing {msg.payload.get_process().name} as a descendent')
                self.node.remove_descendant(msg.payload)

            case _:
                self.process_signal(msg)

    def check_if_blocked(self):
        if self.node.in_tree() and not self.node.get_descendants():
            print(f'{self.name}: Becoming disengaged')
            ancestor_node = self.node.get_ancestor()
            msg = Signal(self, ancestor_node.get_process(), SignalActions.KILL_NODE, self.node)
            self.send(msg)
            self.node.leave_tree()
            self.state = SimulationStates.DISENGAGED

    def grow_tree(self, new_message):
        print(f'{self.name}: Becoming engaged, ancestor is {new_message.sender.name}')
        ancestor_node = new_message.sender.get_node()
        self.node.join_tree(ancestor_node)
        msg = Signal(self, new_message.sender, SignalActions.NEW_NODE, self.node)
        self.send(msg)
        self.state = SimulationStates.ENGAGED

    def get_state(self): 
        return self.statemachine.get_state()

    def send(self, msg):
        self.mailbox.push_to_outbox(msg)
    
    def receive_signal(self, signal):
        self.mailbox.push_signal(signal)

    def receive_event(self, event):
        self.mailbox.push_event(event)
    
    def get_address(self):
        return self

    def link_to(self, actor):
        print(f'{self.name} is now linked to {actor.name}')
        self.mailbox.connect_sender(actor.get_address())

    def initialize(self):
       pass 

    @abstractmethod
    def process_signal(self, msg):
        ...
    
    @abstractmethod
    def engaged(self):
        ...
    
    @abstractmethod
    def disengaged(self):
        ...

class Scheduler(Immaterial):
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

class Controller(Immaterial):
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
        pass

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
