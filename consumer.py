from socket import * 
from class_consumer import Consumer
import sys

serverName = 'localhost' 
serverPort = 12001

clientSocket = socket(AF_INET, SOCK_STREAM) 
clientSocket.connect((serverName , serverPort)) 
print('[CONNECTED]')
clientSocket.send('consumer'.encode()) 
inconsumer=clientSocket.recv(1024).decode()

id=sys.argv[1]
topic=sys.argv[2]
if len(sys.argv) == 4: beg=sys.argv[3]
else: beg='--from-created'


consumer=Consumer(id,topic,beg)

consumer.send_consumer_data(clientSocket)

while True:
        retSentence = clientSocket.recv(1024) 
        clientSocket.send('content received'.encode())
        print('->', retSentence.decode()) 
    


# modifiedSentence = clientSocket.recv(1024) 
# print(' consumer complete ', modifiedSentence.decode()) 
clientSocket.close( )

