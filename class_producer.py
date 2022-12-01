class Producer:
    def __init__(self,id,topic_name) -> None:
        self.id=id
        self.topic_name=topic_name
        
    
    
    def send_prod_Data(self,clientSocket):
        
        rcv=clientSocket.recv(1024).decode()
        # print(rcv)

        clientSocket.send(self.id.encode()) 
        rcv=clientSocket.recv(1024).decode()
        # print(rcv)
        
        clientSocket.send(self.topic_name.encode()) 
        rcv=clientSocket.recv(1024).decode()
        # print(rcv)

 
        