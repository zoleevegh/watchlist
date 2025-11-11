from ib_insync import *

# IBKR-hez csatlakozás
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)  # ha LIVE-ot használsz, 7496 a port

# Példa ticker
contract = Stock('AAPL', 'SMART', 'USD')

# Lekérés
ticker = ib.reqMktData(contract, '', False, False)
ib.sleep(2)  # kis várakozás, amíg jönnek az adatok

print("Last:", ticker.last)
print("Open:", ticker.open)
print("Close:", ticker.close)

ib.disconnect()
