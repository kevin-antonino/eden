from abc import ABC, abstractmethod
from enum import Enum, auto
from infrastructure.util import StateMachine, TreeNode
from infrastructure.actor import Actor
from infrastructure.messages import *

class NodeStates(Enum):
    ENGAGED     = auto()
    DISENGAGED  = auto()
    FINISHING   = auto()

class NodeStateMachine(StateMachine):
    def __init__(self):
        super().__init__()
        self.add_state(NodeStates.ENGAGED, self.engaged_transition)
        self.add_state(NodeStates.DISENGAGED, self.disengaged_transition)
        self.set_init_state(NodeStates.ENGAGED)

    def engaged_transition(self, trig_txt):
        if trig_txt == 'LEAVING_TREE':
            return NodeStates.DISENGAGED
        elif trig_txt == 'FINISH':
            return NodeStates.FINISHING
        else:
            return None

    def disengaged_transition(self, trig_txt):
        if trig_txt == 'JOINING_TREE':
            return NodeStates.ENGAGED
        else:
            return None


class Node(Actor):
    def __init__(self):
        self.name = ''
        self.statemachine = NodeStateMachine()
        self.mailbox = Mailbox()
        self.node = TreeNode(self)
    
    def execute(self):
        match self.get_state():
            case NodeStates.ENGAGED:
                self.flush_signals()
                self.engaged()

            case NodeStates.DISENGAGED:
                self.flush_signals()
                self.disengaged()

            case NodeStates.FINISHING:
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
            self.state = NodeStates.DISENGAGED

    def grow_tree(self, new_message):
        print(f'{self.name}: Becoming engaged, ancestor is {new_message.sender.name}')
        ancestor_node = new_message.sender.get_node()
        self.node.join_tree(ancestor_node)
        msg = Signal(self, new_message.sender, SignalActions.NEW_NODE, self.node)
        self.send(msg)
        self.state = NodeStates.ENGAGED

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

