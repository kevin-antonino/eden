from abc import ABC, abstractmethod
from messages import *

class Scheduler(ABC):
    def __init__(self):
        self.name = ''
        self.mailbox = Mailbox()
        self.node = TreeNode(self)
        self.scheduled_event = None
        self.bypass: bool = False # use an enum state
    
    def execute(self):
        self.flush_signals()
        if not self.scheduled_event:
            self.schedule()
    
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
        if not self.node.in_tree():
            self.grow_tree(self.scheduled_event)
        if self.bypass:
            self.bypass = False # reset flag if used

    def get_next_event(self):
        next_event = self.scheduled_event
        if self.scheduled_event:
            self.scheduled_event = None # reset
        return next_event

    def check_if_blocked(self):
        if self.node.in_tree() and not self.node.get_descendants():
            print(f'{self.name}: Becoming disengaged')
            ancestor_node = self.node.get_ancestor()
            msg = Signal(self, ancestor_node.get_process(), SignalActions.KILL_NODE, self.node)
            self.send(msg)
            self.node.leave_tree()

    def grow_tree(self, new_message):
        print(f'{self.name}: Becoming engaged, ancestor is {new_message.sender.name}')
        ancestor_node = new_message.sender.get_node()
        self.node.join_tree(ancestor_node)
        msg = Signal(self, new_message.sender, SignalActions.NEW_NODE, self.node)
        self.send(msg)
    
    def process_signal(self, msg):
        match msg.action:
            case SignalActions.NEW_NODE:
                print(f'{self.name}: Adding {msg.payload.get_process().name} as a descendent')
                self.node.add_descendant(msg.payload)

            case SignalActions.KILL_NODE:
                print(f'{self.name}: Removing {msg.payload.get_process().name} as a descendent')
                self.node.remove_descendant(msg.payload)

            case SignalActions.UNBLOCK:
                print(f'{self.name} Unblocking....')
                self.bypass = True

            case SignalActions.EARLIEST_EVENT_REQUEST: # Physical process only
                timestamp = self.mailbox.get_next_event_time()
                dt = 1/self.frequency
                msg = Signal(self, self.controller, SignalActions.EARLIEST_EVENT_REQUEST, (timestamp, timestamp + dt, self))
                self.send(msg)
        
    def send(self, msg):
        self.mailbox.push_to_outbox(msg)
    
    def receive_event(self, event):
        self.mailbox.push_event(event)
    
    def receive_signal(self, signal):
        self.mailbox.push_signal(signal)

    @abstractmethod
    def safe_event(self, inbox):
        ...

class ConservativeScheduler(Scheduler):
    def safe_event(self, inbox):
        if all(inbox.values()): # If there is a message waiting from all LPs
            return True
        else:
            return False

class FreeScheduler(Scheduler):
    def safe_event(self, inbox):
        if any(inbox.values()): # If there is a message waiting 
            return True
        else:
            return False

class TreeNode():
    def __init__(self, p):
        self.ancestor = None 
        self.process = p # address to process
        self.descendants = set() # descendant nodes in tree

    def add_descendant(self, node): # Maybe do root/leaf terminology 
        if self in node.get_descendants():
            raise ValueError('Attempting to add make an ancestor a descendant!')
        else:
            self.descendants.add(node)

    def join_tree(self, ancestor_node: "TreeNode"):
        if self.ancestor: 
            raise ValueError('Node already in tree!')
        self.ancestor = ancestor_node

    def leave_tree(self):
        if self.descendants:
            raise ValueError('Attempting to disengage with descendants in tree!')
        self.ancestor = None

    def in_tree(self):
        if self.ancestor or self.descendants:
            return True
        else:
            return False

    def get_descendants(self):
        return self.descendants

    def get_ancestor(self):
        return self.ancestor
    
    def remove_descendant(self, node):
        self.descendants.remove(node)

    def get_process(self):
        return self.process

