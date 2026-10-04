from modeling.model import Model
from numpy import sin, array, nan

class Impulse(Model):
    def __init__(self, t1: float = 0, name: str ='n/a'):
        self.t1 = t1
        super().__init__()

    def evolve(self) -> None:
        self.output = 0
        if (self.get_timestamp() - self.TIME_TOL) < self.t1 and self.get_next_timestamp() > self.t1:
            self.output = self.frequency

class Step(Model):
    def __init__(self, t_step: float = 0, step: float = 1, name: str ='n/a'):
        self.t1 = t_step
        self.step = step
        super().__init__()

    def evolve(self) -> None:
        if (self.get_timestamp() - self.TIME_TOL) < self.t1:
            self.output = self.step

class Ramp(Model):
    def __init__(self, t_ramp: float = 0, slope: float = 1, name: str ='n/a'):
        self.t1 = t_ramp
        self.slope = slope
        super().__init__()

    def evolve(self) -> None:
        if (self.get_timestamp() - self.TIME_TOL) < self.t1:
            self.output += self.slope / self.frequency # might have numerical errors

class Sine(Model):
    def __init__(self, amp: float = 1, freq_hz: float = 1, phase_rad: float = 0, name: str ='n/a'):
        self.A      = amp
        self.omega  = freq_hz
        self.phi    = phase_rad
        super().__init__()

    def evolve(self) -> None:
        self.output = array([[self.A * sin(self.omega * self.get_next_timestamp() + self.phi)]])
    
    def initialize(self):
        self.output = array([[self.A * sin(self.omega * self.get_timestamp() + self.phi)]])

class Sampler(Model):
    def evolve(self) -> None:
        self.output = self.input

    def initialize(self):
        self.output = array([[nan]])
