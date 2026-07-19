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
    timestamp: float = 0

class Mailbox():
    def __init__(self):
        self.links: dict["Process", deque] = {}
        self.inbox = PriorityQueue()
        self.outbox = deque()
        self.count = 0

    def receive_message(self, msg: Message):
        if msg.sender not in self.links:
            raise ValueError(f'{msg.sender.name} not in {msg.receiver.name}\'s inbox!') 

        if not self.links[msg.sender]:
            self.inbox.put((msg.timestamp, self.count, msg))
            self.count += 1

        self.links[msg.sender].append(msg)

    def add_to_outbox(self, msg):
        self.outbox.append(msg)

    def get_next_message(self):
        next_msg = None
        if not self.inbox.empty() and self.inbox.qsize() == len(self.links.keys()):
            _, _, next_msg = self.inbox.get()
            self.links[next_msg.sender].popleft()
            # If there is another message in the queue, put it into the inbox
            queue = self.links[next_msg.sender]
            if queue:
                self.inbox.put((msg.timestamp, self.count, next_msg))
                self.count += 1

        return next_msg

    def remove_process(self, p):
        self.links.pop(p)
   
    def add_sender(self, sender):
        self.links[sender] = deque()

    def get_outbox(self):
        return self.outbox

class ControllerMailbox(Mailbox):
    def __init__(self):
        super().__init__()

    def receive_message(self, msg: Message):
        self.inbox.put((msg.timestamp, self.count, msg))
        self.count += 1

    def get_next_message(self):
        next_msg = None
        if not self.inbox.empty():
            _, _, next_msg = self.inbox.get()

        return next_msg

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
                self.mailboxes[msg.receiver].receive_message(msg)
                print(f'Sending {msg.action} message to {msg.receiver.name} ')
