from collections.abc import Callable
from abc import ABC, abstractmethod
from enum import Enum, auto

class StateMachine(ABC):
    def __init__(self):
        self.state = None
        self.transition_map = {}

    def __str__(self):
        return self.state.name

    def add_state(self, name: Enum, transition: Callable[[str], Enum | None] = None):
        #if transition:
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

class TreeNode():
    def __init__(self, p):
        self.ancestor = None 
        self.process = p # address to process
        self.descendants = set() # descendant nodes in tree

    def add_descendant(self, node): # Maybe do root/leaf terminology 
        if self in node.get_descendants():
            raise ValueError('Attempting to add make an ancestor a descendant!')
        else:
            self.descendants.add(node)

    def join_tree(self, ancestor_node: "TreeNode"):
        if self.ancestor: 
            raise ValueError('Node already in tree!')
        self.ancestor = ancestor_node

    def leave_tree(self):
        if self.descendants:
            raise ValueError('Attempting to disengage with descendants in tree!')
        self.ancestor = None

    def in_tree(self):
        if self.ancestor or self.descendants:
            return True
        else:
            return False

    def get_descendants(self):
        return self.descendants

    def get_ancestor(self):
        return self.ancestor
    
    def remove_descendant(self, node):
        self.descendants.remove(node)

    def get_process(self):
        return self.process

