from abc import ABC, abstractmethod
from processes import *

class Message(ABC):
    def __init__(self, sender : Process, receiver : Process):
        self.sender : Process       = sender
        self.receiver : Process     = receiver
        self.timestamp : float      = sender.get_timestamp()

    def send(self) -> None:
        print(f'{self.sender.name}: Sending message to {self.receiver.name} at {self.timestamp}')
        self.receiver.receive_message(self)
        self.sender.count += 1

    def remove(self) -> None:
        self.receiver.next_causal_msg = NullMessage()
        if self. sender not in self.receiver.inbox.keys():
            return

        if self is not self.receiver.inbox[self.sender][0]:
            print(f'Something with this message is fucked up')
        else:
            self.receiver.inbox[self.sender].popleft()

    @abstractmethod
    def read(self) -> None:
        ...
    
    def open(self):
        self.read()
        self.remove()

class NullMessage(Message):
    def __init__(self):
        self.sender = None
        self.receiver = None
        self.timestamp : float = float('inf')

    def read(self):
        pass

    def open(self):
        pass

class OutputMessage(Message):
    def __init__(self, sender: Process, receiver: Process) -> None:
        super().__init__(sender, receiver)
        self.output = self.sender.get_output(self.timestamp) 

    def read(self):
        print(f'{self.receiver.name}: Pulling input from {self.sender.name} valid at {self.timestamp}')
        self.receiver.pull(self.output)

class Begin(Message):
    def __init__(self, sender: Controller, receiver: Process) -> None:
        super().__init__(sender, receiver)
        self.timestamp = self.receiver.t0

    def read(self):
        print(f'{self.receiver.name}: Opened Begin message. Starting at {self.receiver.get_timestamp()}')
        self.receiver.initialize()
        msg = InitializationComplete(self.receiver, self.sender)    
        self.receiver.add_to_outbox(msg)

class Terminate(Message):
    def __init__(self, sender: Controller, receiver: Process) -> None:
        super().__init__(sender, receiver)
        self.timestamp = receiver.tf

    def read(self):
        self.receiver.finish() 
        print(f'{self.receiver.name}: End message Received. {self.receiver.name} is Done!')

class InitializationComplete(Message):
    def __init__(self, sender: Process, receiver: Controller) -> None:
        super().__init__(sender, receiver)

    def read(self):
        msg = Terminate(self.receiver, self.sender)
        self.receiver.add_to_outbox(msg)

class SimulationComplete(Message):
    def __init__(self, sender: Process, receiver: Process) -> None:
        super().__init__(sender, receiver)

    def read(self):
        print(f'{self.receiver.name}: Notified that {self.sender.name} is done!')
        self.receiver.inbox.pop(self.sender)
        self.receiver.outbox.pop(self.sender, 0)

