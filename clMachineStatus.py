class MachineStatus:
    def __init__(self, clMachine=None, clNameMachine=None, clStatus=None, clIpaddr=None, ipPort=None, Cltime=None,
                 clGreen=None, clYellow=None, clRed=None, clError=None, clIDLE=None, clStartIDLE=None, clStartRunTime= None,
                 last_normal_time=None, clStartStopTime=None,  clStartErrorTime={},  last_error_time=None, listError=None,
                 clflag=False, clConnect=None, hasPrintedError=False, clStartStopTime1 = None):
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
        self.last_normal_time = last_normal_time
        self.clStartStopTime = clStartStopTime
        self.clStartErrorTime = clStartErrorTime
        self.last_error_time = last_error_time
        self.hasPrintedError = hasPrintedError
        self.clStartStopTime1 = clStartStopTime1
        self.ListError = listError if listError is not None else []
        