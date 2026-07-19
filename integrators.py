rom abc import ABC, abstractmethod
from typing import Callable
from numpy import ndarray

class Integrator(ABC):
    @abstractmethod
    def integrate_system_dynamics(
        self,
        x0: ndarray,
        u0: ndarray,
        dx_dt: Callable[[ndarray, ndarray, float], ndarray],
        t0: float,
        tf: float
    ) -> ndarray: 
        pass

class ForwardEuler(Integrator):
    def integrate_system_dynamics(
        self,
        x0: ndarray,
        u0: ndarray,
        dx_dt: Callable[[ndarray, ndarray, float], ndarray],
        t0: float,
        tf: float
    ) -> ndarray:
        xf = (tf - t0) * dx_dt(x0, u0, t0) + x0
        return xf
    
class RungeKutta4(Integrator):
    def integrate_system_dynamics(
        self,
        x0: ndarray,
        u0: ndarray,
        dx_dt: Callable[[ndarray, ndarray, float], ndarray],
        t0: float,
        tf: float
    ) -> ndarray:
        dt = (tf - t0)
        k1 = dt * dx_dt(x0, u0, t0)
        k2 = dt * dx_dt(x0 + k1/2, u0, t0 + dt/2)
        k3 = dt * dx_dt(x0 + k2/2, u0, t0 + dt/2)
        k4 = dt * dx_dt(x0 + k3, u0, t0 + dt)
        xf = x0 + 1/6 * (k1 + 2*k2 + 2*k3 + k4)
        return xf

class NullIntegrator(Integrator):
    def integrate_system_dynamics(
        self,
        x0: ndarray,
        u0: ndarray,
        dx_dt: Callable[[ndarray, ndarray, float], ndarray],
        t0: float,
        tf: float
    ) -> ndarray:
        return x0
