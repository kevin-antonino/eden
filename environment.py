from processes import *

class Simulation():
    def __init__(self, phys_pr: set):
        self.processes = phys_pr
        self.controller = Controller() 
        self.service = PostalService()

    def initialize(self):
        print('SIM IS INITIALIZING')
        for p in self.processes:
            p.controller = self.controller
            self.service.register(p) 
            self.controller.link_to(p)
            p.link_to(self.controller)
            self.controller.add_to_queue(p)

        self.service.register(self.controller)
        self.controller.initialize()
        print('SIM WILL BEGIN')

    def start(self):
        self.initialize()
        self.service.deliver() # Deliver all init messages
        queue = self.controller.get_queue()
        while queue:
        #for i in range(1,10):
            print('Controller executing...')
            self.controller.execute()
            self.service.deliver()
            queue = self.controller.get_queue()
            for process in queue:
                print(f'{process.name} executing...')
                process.execute()
            
                self.service.deliver()
                #queue = self.controller.get_queue()

