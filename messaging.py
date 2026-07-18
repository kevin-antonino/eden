from collections import deque
from dataclasses import dataclass
from queue import PriorityQueue
from enum import Enum, auto

class Actions(Enum):
    START           = auto()
    TERMINATE       = auto()
    PULL            = auto() 
    INIT_COMPLETE   = auto()
    SIM_COMPLETE    = auto()

@dataclass(frozen=True)
class Message:
    sender: "Process"
    receiver: "Process"
    action: Actions
    timestamp: float

    def __post_init__(self):
        object.__setattr__(self, 'timestamp', self.sender.timestamp) # yucky

@dataclass(frozen=True)
class Start(Message):
    action: Actions = Actions.START

@dataclass(frozen=True)
class Terminate(Message):
    action: Actions = Actions.TERMINATE

@dataclass(frozen=True)
class InitializationComplete(Message):
    action: Actions = Actions.INIT_COMPLETE

@dataclass(frozen=True)
class SimulationComplete(Message):
    action: Actions = Actions.SIM_COMPLETE

@dataclass(frozen=True)
class OutputMessage(Message):
    action: Actions = Actions.PULL
    output = sender.output

class Mailbox():
    def __init__(self):
        self.links: dict["Process", deque] = {}
        self.inbox = PriorityQueue()
        self.outbox = deque()

    def receive_message(self, msg):
        if msg.sender not in self.links:
            raise ValueError(f'{msg.sender} not in inbox!') 

        if not self.links[msg.sender]:
            self.inbox.put((msg.timestamp, msg))

        self.links[msg.sender].append(msg)

    def add_to_outbox(self, msg):
        self.outbox.append(msg)

    def get_next_message(self):
        next_msg = None
        if self.inbox.qsize() == len(self.links.keys()):
            next_msg = self.inbox.get()
            self.links[next_msg.sender].popleft()
            # If there is another message in the queue, put it into the inbox
            queue = self.links[next_msg.sender]
            if queue:
                self.inbox.put((queue[0].timestamp, queue[0]))

        return next_msg

    def remove_process(self, p):
        self.links.pop(p)
   
    def add_sender(self, sender):
        self.links[sender] = deque()

    def get_outbox(self):
        return self.outbox

class ControllerMailbox(Mailbox):
    def __init__(self):
        super().__init__(self):

    def receive_message(self, msg):
        self.inbox.put((msg.timestamp, msg))

    def get_next_message(self):
        next_msg = None
        if self.inbox:
            next_msg = self.inbox.get()

        return next_msg

class PostalService():
    def __init__(self):
        self.mailboxes: dict["Process", Mailbox] = {}

    def register(self, process):
        self.mailboxes[process] = process.mailbox

    def deliver(self, process):
        msg_queue = self.mailboxes[process].get_outbox()
        while msg_queue:
            msg = msg_queue.pop()
            self.mailboxes[msg.receiver].receive_message(msg)

