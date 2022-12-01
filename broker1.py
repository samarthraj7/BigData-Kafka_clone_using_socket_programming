from socket import * 
from broker_functions import *
from topic import Topic_prod
from cons_topic import Topic_cons
import os,shutil
import threading

WORKING_DIRECTORY= os.getcwd()
THRESHOLD = 7


def main():

    serverPort = 12001
    serverSocket = socket(AF_INET,SOCK_STREAM) 
    serverSocket.bind(('localhost' ,serverPort)) 
    serverSocket.listen(10)
    print('[BROKER1 RUNNING]') 

    clientSocket = socket(AF_INET, SOCK_STREAM) 
    clientSocket.connect(('localhost' , 5566))
    clientSocket.send('im alive'.encode())
    leader=clientSocket.recv(1024).decode() 
    print(leader)


    while True:
        conn, addr = serverSocket.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr,leader,clientSocket))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")



try:
    os.mkdir(WORKING_DIRECTORY+'/broker1')
except Exception as e:
    shutil.rmtree(WORKING_DIRECTORY+'/broker1')
    os.mkdir(WORKING_DIRECTORY+'/broker1')



f=open(WORKING_DIRECTORY+'/broker1/'+'log.txt','w')
f.close()
# clientSocket.send('im alive'.encode())


def handle_client(connectionSocket,addr,leader,clientSocket):


    
    sentence = connectionSocket.recv(1024).decode()


    if sentence =='producer':
        
        print('[PRODUCER CONNECTED]')
        connectionSocket.send('inside add data branch'.encode())
        id=connectionSocket.recv(1024).decode() 
        connectionSocket.send('id recieved : '.encode())
        topic_name=connectionSocket.recv(1024).decode() 
        connectionSocket.send('topic_name recieved : '.encode())
        connected=True
        while connected:
            content=connectionSocket.recv(1024).decode() 
            if content == 'DISCONNECT DISCONNECT DISCONNECT':
                clientSocket.send('im dead'.encode())
                leader=clientSocket.recv(1024).decode()
                connected=False
                break
            print(content)
            connectionSocket.send('content recieved : '.encode())
            new_write=Topic_prod(topic_name)
            new_write.produce(id,content,THRESHOLD,leader)
        retSentence = 'Done'
        connectionSocket.send(retSentence.encode()) 

        connectionSocket.close()
    
    elif sentence=='consumer':
        connectionSocket.send('inside conumer branch'.encode())
        sentence = connectionSocket.recv(1024).decode()
        if sentence =='Consume Data':
            print('[CONSUMER CONNECTED]')

            id=connectionSocket.recv(1024).decode() 
            connectionSocket.send('id recieved '.encode())
            
            topic_name=connectionSocket.recv(1024).decode() 
            connectionSocket.send('topic_name recieved '.encode())

            beg=connectionSocket.recv(1024).decode() 
            connectionSocket.send('beg recieved '.encode())

            while True:
                try:
                    new_read=Topic_cons(id)
                    ret = new_read.consume(topic_name,beg)
                    while ret=='':
                        if beg=='--from-beginning': beg='--from-created'
                        ret = new_read.consume(topic_name,beg)

                    connectionSocket.send(ret.encode()) 
                    rcv=connectionSocket.recv(1024).decode()
                    if beg=='--from-beginning': beg='--from-created'
                
                except Exception as e:
                    pass
            


    
if __name__ == "__main__":
    main()