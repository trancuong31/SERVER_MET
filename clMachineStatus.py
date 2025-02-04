class MachineStatus:
    def __init__(self, clMachine=None, clNameMachine=None, clStatus=None, clIpaddr=None, ipPort=None, Cltime=None,
                 clGreen=None, clYellow=None, clRed=None, clError=None, clIDLE=None, clStartIDLE=None, clStartRunTime= None,
                  clStartStopTime=None,  clStartErrorTime={}, listError=None,
                 clflag=False, clConnect= 0, clStartStopTime1 = None, typeMachine = None, isConnected = False):
        self.clMachine = clMachine
        self.clNameMachine = clNameMachine
        self.clStatus = clStatus
        self.clIpaddr = clIpaddr
        self.ipPort = ipPort
        self.Cltime = Cltime
        self.clGreen = clGreen
        self.clYellow = clYellow
        self.clRed = clRed
        self.clError = clError
        self.clflag = clflag
        self.clConnect = clConnect
        self.clIDLE = clIDLE
        self.clStartIDLE = clStartIDLE
        self.clStartRunTime = clStartRunTime
        self.clStartStopTime = clStartStopTime
        self.clStartErrorTime = clStartErrorTime
        self.clStartStopTime1 = clStartStopTime1
        self.ListError = listError if listError is not None else []
        self.typeMachine = typeMachine
        self.isConnected = isConnected
        