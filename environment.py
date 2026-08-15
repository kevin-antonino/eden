from processes import *

class Simulation():
    def __init__(self, phys_pr: set):
        self.processes = phys_pr
        self.controller = Controller() 
        self.service = PostalService()

    def initialize(self):
        for p in self.processes:
            p.controller = self.controller
            self.service.register(p) 
            self.controller.link_to(p)
            p.link_to(self.controller)
            self.controller.add_to_queue(p)

        self.service.register(self.controller)
        self.controller.initialize()

    def start(self):
        self.initialize()
        queue = self.controller.get_queue()
        while queue:
            self.controller.execute()
            for process in queue:
                process.execute()
            
            self.service.deliver()
            queue = self.controller.get_queue()

