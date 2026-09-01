from collections import deque
from dataclasses import dataclass
from queue import PriorityQueue
from enum import Enum, auto
from typing import Any
from abc import ABC, abstractmethod

class SignalActions(Enum):
    KILL_NODE       = auto()
    NEW_NODE        = auto()
    LOG             = auto()
    UNBLOCK         = auto()
    EARLIEST_EVENT_REQUEST = auto()

class EventActions(Enum):
    START           = auto()
    TERMINATE       = auto()
    DATA_REQUEST    = auto() 
    DATA_PUSH       = auto()
    SIM_COMPLETE    = auto()

@dataclass(frozen=True)
class Signal:
    sender: "Process"
    receiver: "Process"
    action: SignalActions
    payload: Any = None

@dataclass(frozen=True)
class Event:
    sender: "Process"
    receiver: "Process"
    action: EventActions
    timestamp: float 
    payload: Any = None

class Scheduler(ABC):
    def __init__(self):
        self.bypass: bool = False
    
    def unblock(self):
        self.bypass = True # should be a check if there are events in inbox... consider making inbox a data member (scheduler gets inbox every frame)

    def next_event(self, inbox) -> bool:
        if self.safe_event(inbox) or self.bypass:
            self.bypass = False
            return True
        else:
            return False

    @abstractmethod
    def safe_event(self, inbox):
        ...

class ConservativeScheduler(Scheduler):
    def safe_event(self, inbox):
        if all(inbox.values()): # If there is a message waiting from all LPs
            return True
        else:
            return False

class NullScheduler(Scheduler):
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

class Mailbox():
    def __init__(self):
        self.signal_queue = deque()
        self.inbox: dict["Process", deque] = {}
        self.outbox = deque()
        self.head =  PriorityQueue()
        self.count = 0

    def push_signal(self, msg: Signal):
        self.signal_queue.append(msg)

    def push_event(self, msg: Event):
        if msg.sender not in self.inbox:
            raise ValueError(f'{msg.sender.name} not in {msg.receiver.name}\'s inbox!') 

        if not self.inbox[msg.sender]: # No msgs waiting for this LP => update head
            self.head.put((msg.timestamp, self.count, msg))
            self.count += 1

        self.inbox[msg.sender].append(msg)
    
    def push_to_outbox(self, msg):
        self.outbox.append(msg)

    def pop_signal(self):
        signal = None
        if self.signal_queue:
            signal = self.signal_queue.popleft()

        return signal

    def pop_event(self): 
        # Remove and return current earliest message 
        _, _, top_msg = self.head.get()
        self.inbox[top_msg.sender].popleft()
        # Update head if there are messages waiting
        msg_queue = self.inbox[top_msg.sender]
        if msg_queue:
            self.head.put((msg_queue[0].timestamp, self.count, msg_queue[0]))
            self.count += 1
        return top_msg

    def get_next_event_time(self):
        timestamp, _, _, = self.head.queue[0]
        return timestamp

    def connect_sender(self, sender):
        self.inbox[sender] = deque()
    
    def disconnect_sender(self, sender):
        self.inbox.pop(sender)
   
    def get_senders(self):
        return self.inbox.keys()
    
    def get_inbox(self):
        return self.inbox

    def get_outbox(self):
        return self.outbox

class PostalService():
    def __init__(self):
        self.mailboxes: dict["Process", Mailbox] = {}

    def register(self, process):
        self.mailboxes[process] = process.mailbox

    def deliver(self):
        for mb in self.mailboxes.values():
            msg_queue = mb.get_outbox()
            while msg_queue:
                msg = msg_queue.popleft()
                if isinstance(msg, Event):
                    self.mailboxes[msg.receiver].push_event(msg)
                    print(f'{msg.sender.name} is sending {msg.action} message to {msg.receiver.name} at {msg.timestamp}')
                else:
                    self.mailboxes[msg.receiver].push_signal(msg)
                    print(f'{msg.sender.name} is sending {msg.action} message to {msg.receiver.name}')


