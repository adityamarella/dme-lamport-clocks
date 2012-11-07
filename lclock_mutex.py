#!/usr/bin/env python

import select
import socket
import sys
import Queue
import heapq
import time

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


class Node(SockListener):
    
    FILENAME = "out"

    def __init__(self, machine, machine_list):
        self.pid = machine
        self.pid_list = machine_list
        self.ltime = 0
        self.reqtime = 0
        self.local_queue = []
        self.ack_list = {}
        self.lock = 0
        self.mysocket = MySocket(self)
        self.mysocket.start_server(machine=machine)

    def onReceive(self, data):
        ltime, pid, typ = data
        ltime = int(ltime)
        if typ=='Request':
            self.ltime = 1+max(self.ltime, int(ltime))
            heapq.heappush(self.local_queue, [ltime, pid, 0])
            self.mysocket.send_message(pid, '%d,%s,%s'%(self.ltime,self.pid,"ACK"), "ACK")
            self.lock = pid
            print >>sys.stderr, "\nChanging the local timestamp to %d"%self.ltime
        elif typ=='ACK':
            self.ack_list[pid] = ltime
            flag = True
            for p in self.pid_list:
                if p==self.pid:
                    continue
                ack_time = self.ack_list.get(p, -1)
                if ack_time<=self.reqtime:
                    flag = False
            if flag:
                ltime, p, evt = heapq.heappop(self.local_queue)
                if p==self.pid:
                    print "\n*****Granted resource access to current process(%s)*****"%p
                    self.lock = p
            self.ltime = 1+max(self.ltime, int(ltime))
            print >>sys.stderr, "\nChanging the local timestamp to %d"%self.ltime
        elif typ=='Release':
            #self.local_queue = [ item for item in self.local_queue if not (item[0]==ltime and item[1]==pid) ]
            while self.local_queue:
                heapq.heappop(self.local_queue)
            self.ltime = 1+max(self.ltime, int(ltime))
            self.lock = 0
        print >>sys.stderr, "\nLocal timestamp to %d"%self.ltime
        sys.stderr.write('\n[PROMPT]Input(rq:Request, rl:Release)# ')
        return

    def onStandardInput(self, data):
        inp = data.strip()
        if inp=="rq":
            self.request_resource("out")
        elif inp=='rl':
            self.release_resource("out")

    def request_resource(self,filename):
        if self.lock!=0:
            if self.lock!=self.pid:
                print >>sys.stderr, "\n****Resource in use by %s. Access denied!****"%self.lock
            else:
                print >>sys.stderr, "\n****Resource already in use by the current process****"

            return

        for pid in self.pid_list:
            l = [str(self.ltime), self.pid, 'Request']
            if pid==self.pid:
                self.reqtime = self.ltime
                heapq.heappush(self.local_queue, l)
            else:
                heapq.heappush(self.local_queue, l)
                self.mysocket.send_message(pid, ','.join(l), l[2])
        self.ltime = self.ltime + 1
        print >>sys.stderr, "\nLocal timestamp to %d"%self.ltime
 
    def release_resource(self,filename):
        if self.lock!=0 and self.lock!=self.pid:
            print >>sys.stderr, "\n****Resource in use by %s. Access denied!****"%self.lock
            return

        self.local_queue = [ item for item in self.local_queue if not (item[0]==self.ltime and item[1]==self.pid) ]
        for pid in self.pid_list:
            if pid!=self.pid:
                self.mysocket.send_message(pid, '%d,%s,%s'%(self.ltime,self.pid,"Release"), "Release")
        self.ltime = self.ltime + 1
        print >>sys.stderr, "\nLocal timestamp to %d"%self.ltime
        self.lock = 0
        self.ack_list.clear()

def main():

    try:
        machine = sys.argv[1].strip()
        machine_list = sys.argv[2].split(',')
    except:
        print "Wrong command line args, correct usage is python <script>.py <machine> <comma separated process list(machines)>"
        return

    n = Node(machine, machine_list)

    if len(sys.argv)>4:
        import random

        while True:
            l = ["rq\r\n",'rl\r\n']
            n=random.randrange(2)
            sys.stdin.write(l[n])
            sleep(.1)


if __name__=='__main__':
    main()
