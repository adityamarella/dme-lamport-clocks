#!/usr/bin/env python

import sys
import Queue
import heapq
import time
from mysocket import SockListener, MySocket

class Node(SockListener):
    
    FILENAME = "out"

    def __init__(self, machine, machine_list):
        self.pid = machine
        self.pid_list = machine_list
        self.ltime = 0
        self.reqtime = 0
        self.local_queue = []
        self.ack_list = {}
        self.mysocket = MySocket(self)
        self.mysocket.start_server(machine=machine)

    def check_ack_list(self):
        flag = True
        for p in self.pid_list:
            if p==self.pid:
                continue
            ack_time = self.ack_list.get(p, -1)
            if ack_time<=self.reqtime:
                flag = False
        return flag

    def onReceive(self, data):
        ltime, pid, typ = data
        ltime = int(ltime)
        self.ltime = 1+max(self.ltime, int(ltime))

        if typ=='Request':
            heapq.heappush(self.local_queue, [str(ltime), pid, "Request"])
            self.mysocket.send_message(pid, '%d,%s,%s'%(self.ltime,self.pid,"ACK"), "ACK")
            print >>sys.stderr, "\nChanging the local timestamp to %d"%self.ltime
        elif typ=='ACK':
            self.ack_list[pid] = ltime
            if self.check_ack_list()==True:
                ltime, p, evt = heapq.heappop(self.local_queue)
                if p==self.pid:
                    print "\n*****Granted resource access to current process(%s)*****"%p
                else:
                    heapq.heappush(self.local_queue, [ltime, p, evt])
            print >>sys.stderr, "\nChanging the local timestamp to %d"%self.ltime
        elif typ=='Release':
            #release the pid accessing critical section which will be on the top
            self.ack_list[pid] = ltime
            if self.local_queue:
                heapq.heappop(self.local_queue)

            #check if next pid on the heap is current pid, if it is current pid then grant access
            if self.local_queue:
                ltime, p, evt = heapq.heappop(self.local_queue)
                if p==self.pid and self.check_ack_list()==True:
                    print "\n*****Granted resource access to current process(%s)*****"%p
                else:
                    heapq.heappush(self.local_queue, [ltime, p, evt])

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
        for pid in self.pid_list:
            if pid==self.pid:
                heapq.heappop(self.local_queue)
            else:
                self.mysocket.send_message(pid, '%d,%s,%s'%(self.ltime,self.pid,"Release"), "Release")
        self.ltime = self.ltime + 1
        print >>sys.stderr, "\nLocal timestamp to %d"%self.ltime
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
