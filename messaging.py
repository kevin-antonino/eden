from collections import deque
from dataclasses import dataclass
from enum import Enum, auto

class Actions(Enum):
    START           = auto()
    TERMINATE       = auto()
    PULL            = auto() 
    INIT_COMPLETE   = auto()
    SIM_COMPLETE    = auto()

@dataclass(frozen=True)
class Message:
    self.sender: "Process"
    self.receiver: "Process"
    self.action: Action 
    self.timestamp: float

    def __post_init__:
        self.timestamp = self.sender.timestamp

@dataclass(frozen=True)
class Start(Message):
    self.action: Actions.START

@dataclass(frozen=True)
class Terminate(Message):
    self.action: Actions.TERMINATE

@dataclass(frozen=True)
class InitializationComplete(Message):
    self.action: Actions.INIT_COMPLETE

@dataclass(frozen=True)
class SimulationComplete(Message):
    self.action: Actions.SIM_COMPLETE

@dataclass(frozen=True)
class OutputMessage(Message):
    self.action: Actions.PULL
    self.output = sender.output

class Mailbox():
    def __init__(self):
        self.inbox: dict["Process", deque[Message]] = {} 
        self.outbox: dict["Process", deque[Message]] = {} 

    def receive_message(self, msg):
        if msg.sender not in self.inbox.keys():
            raise ValueError(f'{msg.sender} not in inbox!') 
        self.inbox[msg.sender].append(msg)

    def get_next_message(self):
        next_msg = None
        next_timestamp = float('inf')
        for p in self.inbox.keys():
            if not self.inbox[p]:
                break
            if self.inbox[p][0].timestamp < next_timestamp:
                next_msg = self.inbox[p][0]
                next_timestamp = next_msg.timestamp
        if next_msg is not None:
            self.inbox[next_msg.sender].popleft()

        return next_msg

    def remove_process(self, p):
        self.inbox.pop(self.sender)
        self.outbox.pop(self.sender, 0)
   
    def add_sender(self, sender):
        self.inbox[sender] = deque()

    def add_receiver(self, receiver):
        self.outbox[receiver] = deque()

class PostalService():
    

