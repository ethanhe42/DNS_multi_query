
name_server_list = [
    '114.114.114.114',
    '202.117.0.20'
]

name_server = name_server_list[1]
f = open("malurl.txt")
maxthreads = 300

from sets import Set
import dns.resolver
import dns.name
import dns.message
import dns.query
import dns.rdatatype

import Queue
import threading
import time

additZero=0
wrongCnt = 0
registered=0

def query(domain, name_server):
    domain = dns.name.from_text(domain)
    if not domain.is_absolute():
        domain = domain.concatenate(dns.name.root)

    request = dns.message.make_query(domain, dns.rdatatype.A)
    response = dns.query.udp(request, name_server, timeout=1)

    return response


domains = Set([])

for line in f.readlines():
    
    domains.add(str(line).split()[0])

f = open("maxTTL.txt", mode='w')

q = Queue.Queue()


def getTTL(domain, q):
    global wrongCnt,additZero,registered
    TTL = 0
    try:
        response = query(domain, name_server)
        additCnt = len(response.additional)
    except dns.exception.Timeout:
        additCnt = 0
        wrongCnt += 1

    if additCnt == 0:
        additZero+=1

    else:
        addit = response.additional[0]
        Serveraddr = addit.items[0].address

        try:
            response = query(domain, Serveraddr)
            ansCnt = len(response.additional)
        except dns.exception.Timeout:
            ansCnt = 0
            wrongCnt += 1

        ansCnt = len(response.answer)
        if ansCnt != 0:
            registered+=1
            answer = response.answer[0]
            TTL = answer.ttl

    # print domain, TTL
    # f.write(domain + ' ' + str(TTL) + '\n')
    if TTL!=0:
        q.put(domain + ' ' + str(TTL))


numofthreads = 0
threadspool = []
finish=0

for domain in domains:
    finish+=1
    t = threading.Thread(target=getTTL, args=(domain, q))
    t.daemon = True
    threadspool.append(t)
    t.start()

    numofthreads += 1
    if numofthreads > maxthreads:
        numofthreads = 0



        while not q.empty():
            line = q.get()
            #print(line)

            f.write(line + '\n')
        #print additZero
        print str(registered)+'/'+str(finish)+'/'+str(len(domains))

while len(threadspool) != 0:            
    threadspool.pop().join()

time.sleep(3)
f.close()
