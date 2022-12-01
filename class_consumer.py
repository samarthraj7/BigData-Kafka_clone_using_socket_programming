from socket import * 

class Consumer:
    def __init__(self,id,topic_name,beg) -> None:
        self.id=id
        self.topic_name=topic_name
        self.beg=beg

    def send_consumer_data(self,clientSocket):
        sentence='Consume Data'
        clientSocket.send(sentence.encode()) 

        clientSocket.send(self.id.encode())
        rcv=clientSocket.recv(1024).decode() 
        print(rcv)

        clientSocket.send(self.topic_name.encode())
        rcv=clientSocket.recv(2048).decode() 
        print(rcv)

        clientSocket.send(self.beg.encode())
        rcv=clientSocket.recv(1024).decode() 
        print(rcv)

        
