from abc import ABC, abstractmethod
from enum import Enum, auto
from states import StateMachine

class NodeStates(Enum):
    ENGAGED     = auto()
    DISENGAGED  = auto()
    FINISHING   = auto()

class NodeStateMachine(StateMachine):
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


class Node(Actor):
    def __init__(self):
        self.name = ''
        self.statemachine = SimulationStateMachine()
        self.mailbox = Mailbox()
        self.node = TreeNode(self)
    
    def execute(self):
        match self.get_state():
            case SimulationStates.ENGAGED:
                self.flush_signals()
                self.engaged()

            case SimulationStates.DISENGAGED:
                self.flush_signals()
                self.disengaged()

            case SimulationStates.FINISHING:
                self.flush_signals()
                self.finishing()

    def flush_signals(self):
        next_signal = self.mailbox.pop_signal()
        while next_signal:
            self.handle_signal(next_signal)
            next_signal = self.mailbox.pop_signal()

    def handle_signal(self, msg):
        match msg.action:
            case SignalActions.NEW_NODE:
                print(f'{self.name}: Adding {msg.payload.get_process().name} as a descendent')
                self.node.add_descendant(msg.payload)

            case SignalActions.KILL_NODE:
                print(f'{self.name}: Removing {msg.payload.get_process().name} as a descendent')
                self.node.remove_descendant(msg.payload)

            case _:
                self.process_signal(msg)

    def check_if_blocked(self):
        if self.node.in_tree() and not self.node.get_descendants():
            print(f'{self.name}: Becoming disengaged')
            ancestor_node = self.node.get_ancestor()
            msg = Signal(self, ancestor_node.get_process(), SignalActions.KILL_NODE, self.node)
            self.send(msg)
            self.node.leave_tree()
            self.state = SimulationStates.DISENGAGED

    def grow_tree(self, new_message):
        print(f'{self.name}: Becoming engaged, ancestor is {new_message.sender.name}')
        ancestor_node = new_message.sender.get_node()
        self.node.join_tree(ancestor_node)
        msg = Signal(self, new_message.sender, SignalActions.NEW_NODE, self.node)
        self.send(msg)
        self.state = SimulationStates.ENGAGED

    def get_state(self): 
        return self.statemachine.get_state()

    def send(self, msg):
        self.mailbox.push_to_outbox(msg)
    
    def receive_signal(self, signal):
        self.mailbox.push_signal(signal)

    def receive_event(self, event):
        self.mailbox.push_event(event)
    
    def get_address(self):
        return self

    def link_to(self, actor):
        print(f'{self.name} is now linked to {actor.name}')
        self.mailbox.connect_sender(actor.get_address())

    def finish(self):
        self.statemachine.trigger('FINISH')    

    def initialize(self):
       pass 

    @abstractmethod
    def process_signal(self, msg):
        ...
    
    @abstractmethod
    def engaged(self):
        ...
    
    @abstractmethod
    def disengaged(self):
        ...
    
    @abstractmethod
    def finishing(self):
        ...

