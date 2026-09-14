from abc import ABC
from states import SimulationStates, SimulationStateMachine
from scheduling import TreeNode
from messages import *

class Actor(ABC):
    def __init__(self):
        self.name = ''

    def __str__(self):
        return self.name

    # main
    @abstractmethod
    def execute(self) -> None:
        ...

    # messaging 
    @abstractmethod
    def send(self):
        ...

    @abstractmethod
    def get_address(self):
        ...

    # start / finish
    @abstractmethod
    def initialize(self):
        ...

    @abstractmethod
    def finish(self):
        ...

