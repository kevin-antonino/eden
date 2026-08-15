from dataclasses import dataclass, field
import numpy as np
import matplotlib.pyplot as plt

@dataclass
class PlotConfig:
    ticks: Dict[str, Any] = field(default_factory=lambda: {
        'axis': 'both',
        'which': 'major',
        'direction': 'in',
        'length': 4,
        'width': 2,
        'colors': 'black'
    })
    grid: Dict[str, Any] = field(default_factory=lambda: {
        'visible': True,
        'which': 'major',
        'axis': 'both',
        'linewidth': 0.8,
        'color': 'grey',
        'alpha': 0.7
    })
    linewidth: int = 2
    title_fontsize: int = 14
    label_fontsize: int = 12
    facecolor: str = '#ebebeb'

def plot_trajectory(traj, time, label):
    (n_plots, _) = traj.shape
    fig, axes = plt.subplots(n_plots, 1) 

    axes = [axes] if n_plots == 1 else axes
    
    for i in range(0, n_plots):
        ylab = f'${label}_{i}$'
        plot_on_ax(axes[i], time, traj[i,:], 
            ylabel=ylab, xlabel='Time [s]')

    fig.patch.set_facecolor('lightgray')
    plt.show()

def plot_on_ax(ax: matplotlib.axes.Axes, 
    x: ndarray, y: ndarray, title: str = '', 
    ylabel: str='', xlabel: str='') -> None:

    cfg = PlotConfig()
    ax.set_title(title, fontsize=cfg.title_fontsize)
    ax.set_xlabel(xlabel, fontsize=cfg.label_fontsize)
    ax.set_ylabel(ylabel, fontsize=cfg.label_fontsize)
    ax.set_facecolor(cfg.facecolor)
    ax.grid(**cfg.grid)
    ax.minorticks_on()
    ax.tick_params(**cfg.ticks)
    ax.plot(x, y, color='blue', linewidth=cfg.linewidth)
