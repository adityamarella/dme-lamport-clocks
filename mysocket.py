
import sys
import select
import socket

class SockListener:

    def onReceive(data):
        pass

    def onStandardInput(data):
        pass

    def onError():
        pass

class MySocket:

    def __init__(self, listener):
        self.listener = listener
        pass

    def start_server(self, machine='localhost:10000'):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setblocking(0)

        addr, port = machine.split(":")
        # Bind the socket to the port
        server_address = (addr, int(port))
        print >>sys.stderr, 'starting up on %s port %s' % server_address
        server.bind(server_address)

        # Listen for incoming connections
        server.listen(5)

        # Sockets from which we expect to read
        inputs = [ sys.stdin, server ]

        # Sockets to which we expect to write
        outputs = [ ]

        message_queues = {}
        sys.stderr.write('\n[PROMPT]Input(rq:Request, rl:Release)# ')

        while inputs:

            # Wait for at least one of the sockets to be ready for processing
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            # Handle inputs
            for s in readable:

                if s is sys.stdin:
                    self.listener.onStandardInput(s.readline())
                    sys.stderr.write('\n[PROMPT]Input(rq:Request, rl:Release)# ')

                elif s is server:
                    # A "readable" server socket is ready to accept a connection
                    connection, client_address = s.accept()
                    #print >>sys.stderr, 'new connection from', client_address
                    connection.setblocking(0)
                    inputs.append(connection)

                    self.onConnect(client_address)

                    # Give the connection a queue for data we want to send
                    message_queues[connection] = Queue.Queue()
                else:
                    data = s.recv(1024)
                    if data:
                        # A readable client socket has data
                        #print >>sys.stderr, 'received "%s" from %s' % (data, s.getpeername())
                        message_queues[s].put(data)
                        # Add output channel for response
                        if s not in outputs:
                            outputs.append(s)
                        self.onReceive(client_address, data)
                    else:
                        # Interpret empty result as closed connection
                        #print >>sys.stderr, 'closing', client_address, 'after reading no data'
                        # Stop listening for input on the connection
                        if s in outputs:
                            outputs.remove(s)
                        inputs.remove(s)
                        s.close()

                        # Remove message queue
                        del message_queues[s]
          
            #no need of sending any data back to requestor 
            for s in writable:
                pass

            #    try:
            #        #next_msg = message_queues[s].get_nowait()
            #        pass
            #    except Queue.Empty:
            #        # No messages waiting so stop checking for writability.
            #        print >>sys.stderr, 'output queue for', s.getpeername(), 'is empty'
            #        outputs.remove(s)
            #    else:
            #        print >>sys.stderr, 'sending "%s" to %s' % (next_msg, s.getpeername())
            #        s.send(next_msg)

            # Handle "exceptional conditions"
            for s in exceptional:
                print >>sys.stderr, 'handling exceptional condition for', s.getpeername()
                # Stop listening for input on the connection
                inputs.remove(s)
                if s in outputs:
                    outputs.remove(s)
                s.close()

                # Remove message queue
                del message_queues[s]
        server.close()

    def onConnect(self, client_address):
        pass 

    def onReceive(self, client_address, data):
        print >>sys.stderr, "\nReceived message %s"%data
        if self.listener!=None:
            self.listener.onReceive([ item.strip() for item in data.split(',')])

    def send_message(self, machine, message, typ):

        addr, port = machine.split(':')
        server_address = (addr, int(port))
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket to the port where the server is listening
        print >>sys.stderr, '\nSending %s message(%s) to %s' %(typ, message, machine)

        try:
            sock.connect(server_address)
            sock.send(message)
        except:
            print >>sys.stderr, "\n*****Send failure. Node '%s' is down. Aborting*****"%machine
            exit(1)

        sock.close()

