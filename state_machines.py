from collections.abc import Callable
from abc import ABC, abstractmethod
from enum import Enum

class SimulationStates(Enum):
    ENGAGED     = auto()
    DISENGAGED  = auto()

class ModelStates(Enum):
    REQUESTING  = auto() 
    WAITING     = auto() 
    EVOLVING    = auto() 

class StateMachine(ABC):
    def __init__(self):
        self.state = None
        self.transition_map = {}

    def add_state(self, name: Enum, transition: Callable[[str], Enum | None]):
        self.transition_map[name] = transition
   
    def set_init_state(self, desired_state: str):
        if desired_state not in self.transition_map.keys():
            raise ValueError('Init state not in list of states!')
        self.state = desired_state

    def trigger(self, trig_txt: str):
        transition = self.transition_map[self.state]
        next_state = transition(trig_txt)    
        if next_state not in self.transition_map.keys():
            raise ValueError(f'{state} isn\'t a state in this machine')
        self.state = next_state

    def get_state(self):
        return self.state

class ModelStateMachine(StateMachine):
    def __init__(self):
        super().__init__()
        self.add_state(ModelStates.REQUESTING, self.requesting_transition)
        self.add_state(ModelStates.WAITING, self.waiting_transition)
        self.add_state(ModelStates.EVOLVING, self.evolving_transition)
        self.set_init_state(ModelStates.WAITING)

    def requesting_transition(self, trig_txt):
        if trig_txt == 'DATA_READY':
            return ModelStates.WAITING
        else:
            return None

    def waiting_transition(self, trig_txt):
        if trig_txt == 'NEED_DATA':
            return ModelStates.REQUESTING

        elif trig_txt == 'INCREMENT_TIME':
            return ModelStates.EVOLVING
        else:
            return None

    def evolving_transition(self, trig_txt):
        if trig_txt == 'EVOLVE_DONE':
            return ModelStates.REQUESTING
        else:
            return None

class SimulationStateMachine(StateMachine):
    def __init__(self):
        super().__init__()
        self.add_state(ModelStates.ENGAGED, self.engaged_transition)
        self.add_state(ModelStates.DISENGAGED, self.disengaged_transition)
        self.set_init_state(ModelStates.DISENGAGED)

    def engaged_transition(self, trig_txt):
        if trig_txt == 'LEAVING_TREE':
            return SimulationStates.DISENGAGED
        else:
            return None

    def disengaged_transition(self, trig_txt):
        if trig_txt == 'JOINING_TREE'
            return SimulationStates.ENGAGED
        else:
            return None

    
## EXAMPLE CODE FOR MODEL OBJECT
statemachine = ModelStateMachine()
input_valid = {}

def get_state(self):
    return self.statemachine.get_state()

def process_available_events(self):
    if self.scheduler.get_next_event_time() < self.get_next_timestamp(): 
        event = self.scheduler.pop_next_event()
        self.process_event(event)

def inputs_ready(self):
    if self.input_valid is None or all(self.input_valid.values()):
        return True
    else:
        return False

def execute(self):
    match self.get_state():
        case ModelStates.REQUESTING: # Model is blocked & waiting for an input
            self.process_available_events()
            if self.inputs_ready():
                self.statemachine.trigger('DATA_READY')
        
        case ModelStates.WAITING: # Input data is valid, model is waiting and processing events within [t, t+dt)
            self.process_available_events()
            if not self.inputs_ready(): 
                self.statemachine.trigger('NEED_DATA')
            
            if self.scheduler.get_next_event_time() >= self.get_next_timestamp():
                self.statemachine.trigger('INCREMENT_TIME')

        case ModelStates.EVOLVING: # Model progressing time
            self.evolve()
            self.increment_time()
            if self.logger:
                self.log()
            self.statemachine.trigger('EVOLVE_DONE')

