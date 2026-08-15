from processes import PhysicalProcess
from integrators import RungeKutta4
from abc import abstractmethod
from numpy import zeros

class DynamicSystem(PhysicalProcess):
    def __init__(self, n_states: int, n_inputs: int, n_outputs: int):
        super().__init__()
        self.integrator = RungeKutta4() 
        self.state  = zeros((n_states, 1))
        self.input = zeros((n_inputs, 1))
        self.output = zeros((n_inputs, 1))

    def evolve(self) -> None:
        # Evaluate x[k] = F(x[k-1], u[k-1], k-1) 
        self.state = self.integrator.integrate_system_dynamics(
            self.state, self.input, self.dynamic_equation, 
            self.get_timestamp(), self.get_next_timestamp())
        
        # Evaluate y[k] = G(x[k], u[k], k)
        self.output  = self.output_equation(
            self.state, self.input, self.get_timestamp())

    @abstractmethod
    def dynamic_equation(self, x: ndarray, u: ndarray, t: float) -> ndarray:
        '''
        xdot = f(x, u, t)
        '''
        ...
    
    @abstractmethod
    def output_equation(self, x: ndarray, u: ndarray, t: float) -> ndarray:
        '''
        y = g(x, u, t)
        '''
        ...

class LinearSystem(DynamicSystem):
    def __init__(self, A: ndarray, B: ndarray, C: ndarray, D: ndarray):
        self.A, self.B, self.C, self.D = A, B, C, D # System Matrices
        super().__init__(A.shape[1], B.shape[1], C.shape[0])

    def dynamic_equation(self, x: ndarray, u: ndarray, t: float) -> ndarray:
        xdot = self.A @ x + self.B @ u
        return xdot

    def output_equation(self, x: ndarray, u: ndarray, t: float) -> ndarray: 
        y = self.C @ x + self.D @ u
        return y
