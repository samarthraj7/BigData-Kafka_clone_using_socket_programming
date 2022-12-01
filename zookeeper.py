import socket
import threading
import time
   
active_brokers=[3,2,1]


  
  


IP ='localhost'
PORT = 5566
ADDR = (IP, PORT)


def leader_election():

    return active_brokers[-1]













def handle_client(conn, addr):
    

    connected = True

    print(connected)

    msg = conn.recv(1024).decode()
    conn.send(str(active_brokers[-1]).encode())
    print(msg)

    while connected:

        msg = conn.recv(1024).decode()
        if(msg=='im dead'):

            active_brokers.pop()

            LEADER=leader_election()
            
            conn.send(str(LEADER).encode())
            
            connected=False
                
            



        



        
        
        
        
    
    conn.close()

def main():
    print("[STARTING] Zookeeper is starting...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

if __name__ == "__main__":
    main()