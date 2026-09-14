from collections.abc import Callable
from abc import ABC, abstractmethod
from enum import Enum, auto

class SimulationStates(Enum):
    ENGAGED     = auto()
    DISENGAGED  = auto()
    FINISHING   = auto()

class ModelStates(Enum):
    INITIALIZING = auto()
    PROCESSING   = auto() 
    EVOLVING     = auto() 
    FINISHING    = auto()

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

class ModelStateMachine(StateMachine):
    def __init__(self):
        super().__init__()
        self.add_state(ModelStates.INITIALIZING, self.initializing_transition)
        self.add_state(ModelStates.PROCESSING, self.processing_transition)
        self.add_state(ModelStates.EVOLVING, self.evolving_transition)
        self.set_init_state(ModelStates.INITIALIZING)

    def initializing_transition(self, trig_txt):
        if trig_txt == 'INITIALIZED':
            return ModelStates.PROCESSING
        else:
            return None

    def processing_transition(self, trig_txt):
        if trig_txt == 'INCREMENT_TIME':
            return ModelStates.EVOLVING
        elif trig_txt == 'END_MESSAGE':
            return ModelStates.FINISHING
        else:
            return None

    def evolving_transition(self, trig_txt):
        if trig_txt == 'EVOLVED':
            return ModelStates.PROCESSING
        else:
            return None

class SimulationStateMachine(StateMachine):
    def __init__(self):
        super().__init__()
        self.add_state(SimulationStates.ENGAGED, self.engaged_transition)
        self.add_state(SimulationStates.DISENGAGED, self.disengaged_transition)
        self.set_init_state(SimulationStates.ENGAGED)

    def engaged_transition(self, trig_txt):
        if trig_txt == 'LEAVING_TREE':
            return SimulationStates.DISENGAGED
        elif trig_txt == 'FINISH':
            return SimulationStates.FINISHING
        else:
            return None

    def disengaged_transition(self, trig_txt):
        if trig_txt == 'JOINING_TREE':
            return SimulationStates.ENGAGED
        else:
            return None

    