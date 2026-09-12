from model import *
from infrastructure import *
from messages import *

class Simulation():
    def __init__(self, models: set):
        self.models = models
        self.controller = Controller() 
        self.service = MessageService()

    def initialize(self):
        print('SIMULATION INITIALIZING')
        # Register models to the sim infrastructure
        for model in self.models:
            self.service.register(model) 
            self.controller.link_to(model)
            model.link_to(self.controller)
            self.controller.add_to_queue(model)
            model.initialize()

        # Register controller
        self.service.register(self.controller)
        self.controller.initialize()
        print('SIMULATION INITIALIZATION COMPLETE')

    def run(self):
        self.initialize()
        self.service.deliver() # Deliver all init messages
        queue = self.controller.get_queue()
        #while queue:
        for i in range(1,20):
            print('Controller executing...')
            self.controller.execute()
            self.service.deliver()
            queue = self.controller.get_queue()
            for process in queue:
                print(f'{process.name} executing...')
                process.scheduler.execute()
                process.execute()
                self.service.deliver()
