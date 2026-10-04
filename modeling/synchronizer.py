from enum import Enum, auto
from infrastructure.util import StateMachine

class SyncStates(Enum):
    HOLD      = auto()
    REQUEST   = auto()
    SYNC      = auto()
    PASS      = auto()

class SynchronizerStateMachine(StateMachine):
    def __init__(self):
        super().__init__()
        self.add_state(SyncStates.REQUEST, self.request_transition)
        self.add_state(SyncStates.SYNC, self.sync_transition)
        self.add_state(SyncStates.HOLD, self.hold_transition)
        self.add_state(SyncStates.PASS, self.pass_transition)
        self.set_init_state(SyncStates.REQUEST)

    def request_transition(self, trig_txt):
        if trig_txt == 'REQUEST_SENT':
            return SyncStates.HOLD
        else:
            return None

    def sync_transition(self, trig_txt):
        if trig_txt == 'SYNC_DONE':
            return SyncStates.PASS
        else:
            return None

    def hold_transition(self, trig_txt):
        if trig_txt == 'NEED_SYNC':
            return SyncStates.SYNC
        elif trig_txt == 'READY':
            return SyncStates.PASS
        else:
            return None

    def pass_transition(self, trig_txt):
        if trig_txt == 'NEED_INPUTS':
            return SyncStates.REQUEST
        elif trig_txt == 'NEED_SYNC':
            return SyncStates.HOLD
        elif trig_txt == 'READY':
            return SyncStates.PASS
        else:
            return None

class Synchronizer():
    def __init__(self):
        self.statemachine = SynchronizerStateMachine()
        self.model_validity_horizon = (float('nan'), float('nan'))
        self.input_validity_horizons = {}
        self.output_validity_horizons = {}
        self.sync_times = {}
     
    def update_state(self):
        match self.get_state():
            case SyncStates.REQUEST:
                return

            case SyncStates.HOLD:
                if self.inputs_valid() and self.outputs_valid():
                    if self.need_to_sync():
                        self.statemachine.trigger('NEED_SYNC')
                    else:
                        self.statemachine.trigger('READY')

            case SyncStates.SYNC:
                if not self.need_to_sync():
                    self.statemachine.trigger('SYNC_DONE')

            case SyncStates.PASS:
                if not self.inputs_valid():
                    self.statemachine.trigger('NEED_INPUTS')
                elif not self.outputs_valid():
                    self.statemachine.trigger('NEED_SYNC')
                else:
                    self.statemachine.trigger('READY')

    def set_input_sample(self, model, sample_t0, sample_tf):
        validity_end_time = max(sample_tf, self.model_validity_horizon[1])
        self.input_validity_horizons[model] = (sample_t0, validity_end_time)
        self.update_state()

    def set_output_sample(self, model, sample_t0, sample_tf):
        validity_end_time = max(sample_tf, self.model_validity_horizon[1])
        self.output_validity_horizons[model] = (sample_t0, validity_end_time)
        self.update_state()

    def advance_time(self, tf):
        self.model_validity_horizon = (self.model_validity_horizon[1], tf)
        self.update_state()
    
    def calc_sync_time(self, model):
        if model in self.input_validity_horizons and self.output_validity_horizons:
            self.sync_times[model] = min(self.input_validity_horizons[model][1], self.output_validity_horizons[model][1])
        elif model in self.input_validity_horizons:
            self.sync_times[model] = self.input_validity_horizons[model][1]
        else:
            self.sync_times[model] = self.output_validity_horizons[model][1]
        
        self.update_state()
        return self.sync_times[model]

    def set_input_model(self, model):
        self.input_validity_horizons[model] = (-float('inf'), -float('inf'))
        self.sync_times[model] = -float('inf')

    def set_output_model(self, model):
        self.output_validity_horizons[model] = (-float('inf'), -float('inf'))
        self.sync_times[model] = -float('inf')

    def need_input(self, model):
        return self.input_validity_horizons[model][1] <= self.model_validity_horizon[1]

    def get_request_list(self):
        return [model for model in self.input_validity_horizons.keys() if self.need_input(model)]
    
    def need_sync(self, model):
        return self.sync_times[model] < self.model_validity_horizon[1]

    def get_sync_list(self):
        return [model for model in self.sync_times.keys() if self.need_sync(model)]

    def inputs_valid(self):
        if not self.input_validity_horizons:
            return True
        else:
            return all([in_horiz[1] >= self.model_validity_horizon[1] for in_horiz in self.input_validity_horizons.values()])
    
    def outputs_valid(self):
        if not self.output_validity_horizons:
            return True
        else:
            return all([out_horiz[1] >= self.model_validity_horizon[1] for out_horiz in self.output_validity_horizons.values()])

    def need_to_sync(self):
        if not self.sync_times:
            return False
        else:
            return any([time < self.model_validity_horizon[1] for time in self.sync_times.values()])
    
    def initialize(self, init_time, next_ts):
        self.model_validity_horizon = (init_time, next_ts)

    def request_sent(self):
        self.statemachine.trigger('REQUEST_SENT')
        self.update_state()

    def get_state(self):
        return self.statemachine.get_state()
