from processes import *

class Simulation():
    def __init__(self, phys_pr: set):
        self.processes = phys_pr
        self.controller = Controller() 
        self.service = PostalService()

    def initialize(self):
        for p in self.processes:
            self.service.register(p) 
            self.controller.add_to_queue(p)

        self.controller.initialize()

    def start(self):
        queue = self.controller.get_queue()
        while queue:
            self.controller_check_inbox()
            self.controller.execute()
            for process in queue:
                process.check_inbox()
                process.execute()
            
            self.service.deliver()
            queue = self.controller.get_queue()

