from Client.ClientNodes import ClientNode

module1 = ClientNode.ClientNode("module1", "10.30.8.123", 50010)
module1.start()
# Easiest way to prevent uncaught 'KeyboardInterrupt' errors
try:
    module1.join()
except KeyboardInterrupt:
    pass

"""
This script can be ran in parallel for each client you want to add to the distributed network.
Server nodes will handle these new connections using load balancing and microservice functionality.
"""

