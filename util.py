from collections.abc import Callable
from abc import ABC, abstractmethod
from enum import Enum, auto

class StateMachine(ABC):
    def __init__(self):
        self.state = None
        self.transition_map = {}

    def __str__(self):
        return self.state.name

    def add_state(self, name: Enum, transition: Callable[[str], Enum | None]):
        self.transition_map[name] = transition
   
    def set_init_state(self, desired_state: str):
        if desired_state not in self.transition_map.keys():
            raise ValueError('Init state not in list of states!')
        self.state = desired_state

    def trigger(self, trig_txt: str):
        transition = self.transition_map[self.state]
        next_state = transition(trig_txt)    
        if next_state:
            #if next_state not in self.transition_map.keys():
            #    raise ValueError(f'{next_state.name} isn\'t a state in this machine')
            self.state = next_state
        else:
            self.bad_transition()

    def __call__(self, trig_txt):
        self.trigger(trig_txt)

    def get_state(self):
        return self.state
    
    def bad_transition(self):
        raise ValueError(f'Unknown trigger text for {self}')

