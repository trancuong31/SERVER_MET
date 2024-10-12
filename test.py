import pymcprotocol
from clMap import clMap
import json
# pymc3e = pymcprotocol.Type3E()
# pymc3e.connect("192.168.1.10", 9999)
# pymc3e.batchwrite_wordunits(headdevice="D10", values=[0, 10, 20, 30, 40])
# wordunits_values = pymc3e.batchread_wordunits(headdevice="D10", readsize=10)
# listPLC=clMap()
try:
    with open('data.json', 'r') as file:
        data = json.load(file)
    print(data['ipServer'])
except FileNotFoundError:
    data = {}  # If file doesn't exist, create an empty dictionary
#  {"ip": "192.168.1.25", "port": 9914, "nameMachine":"LB2" , "typeMachine":"ASSEMBLY"}