from infrastructure.controller import *
from infrastructure.messages import *

class Simulation():
    def __init__(self, models: set, loggers: set = set()):
        self.models = models
        self.loggers = loggers
        self.controller = Controller() 
        self.service = MessageService()

    def initialize(self):
        print('SIMULATION INITIALIZING')
        # Register models to the sim infrastructure
        for model in self.models:
            self.service.register(model.name, model.get_address()) 
            self.controller.link_to(model)
            model.link_to(self.controller)
            self.controller.add_to_queue(model)

        for logger in self.loggers:
            self.service.register(logger.name, logger.get_address()) 

        # Register controller
        self.service.register(self.controller.name, self.controller.get_address())
        print('SIMULATION INITIALIZATION COMPLETE')

    def run(self):
        self.initialize()
        self.service.deliver() # Deliver all init messages
        queue = self.controller.get_queue()
        while queue:
            self.controller.execute()
            self.service.deliver()
            queue = self.controller.get_queue()
            for process in queue:
                process.execute()
                self.service.deliver()

            for logger in self.loggers:
                logger.execute()



