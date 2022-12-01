import os,shutil
WORKING_DIRECTORY= os.getcwd()

class Topic_prod:
    instances={}
    def __init__(self,topic_name) -> None:
        self.topic_name=topic_name
        
        if self.topic_name in Topic_prod.instances:
            self.length=Topic_prod.instances[self.topic_name][-1][1]
            self.partition_count=Topic_prod.instances[self.topic_name][-1][2]
        
        else:
            Topic_prod.instances[self.topic_name]=[]
            self.length=0
            self.partition_count=0

    def produce(self,id,content,threshold,leader):
        parent = WORKING_DIRECTORY+"/broker1"
        l = len(content)
        xtra = self.length % threshold
        self.length += l
        directory=self.topic_name
        path = os.path.join(parent, directory)

        try:
            os.mkdir(path)
            os.chdir(path)
            
            
        except Exception as e:
            os.chdir(path)
            
            if xtra:
                filename= str(self.partition_count) + '.txt'
                # print(filename)
                f=open(filename,'a+')
                seek=f.tell()
                #print(seek)
                f.write(content[:(threshold-xtra)])
                
                f.close()
            
                content=content[(threshold-xtra):]
        
                
        finally:
            
            rem = self.length % threshold
            x=1 if rem>0 else 0
            prev_count=self.partition_count
            self.partition_count = self.length//threshold + x
            
            Topic_prod.instances[self.topic_name].append((id,self.length,self.partition_count))
            
            seek=0
            for i in range(prev_count+1,self.partition_count):
                filename= str(i) + '.txt'
                # print(filename)
                f=open(filename,'a+')
                f.write(content[seek:seek+threshold])
                seek += threshold
                f.close()
            if rem:
                filename= str(self.partition_count) + '.txt'
                f=open(filename,'a+')
                # print(filename)
                f.write(content[seek:])
                
                f.close()
            os.chdir(parent)
        f=open('log.txt','a+')
        f.write(str(Topic_prod.instances)+'\n')
        os.chdir(WORKING_DIRECTORY)

        brokers=[1,2,3]
        brokers.remove(int(leader))

        source_folder = WORKING_DIRECTORY+'/broker'+ str(leader)
        destination_folder1 = WORKING_DIRECTORY+'/broker'+ str(brokers[0])
        destination_folder2 = WORKING_DIRECTORY+'/broker'+str(brokers[1])

        shutil.rmtree(destination_folder1)
        shutil.copytree(source_folder, destination_folder1)
        shutil.rmtree(destination_folder2)
        shutil.copytree(source_folder, destination_folder2)


    # def __init__(self,name,partition_count=0,length=0) -> None:
    #     self.name=name
        
    #     self.partition_count=partition_count
    #     self.length=length
    #     topic.instances.append(self)
    
        
    # def partitionWrite(self,content,threshold):
    #     parent="C:/Users/rohan/Desktop/BDLAB/consumer_build/broker1"
    #     l = len(content)
    #     xtra = self.length % threshold
    #     self.length += l
    #     directory=self.name
    #     path = os.path.join(parent, directory)
    #     print(self.partition_count)
        
        
    #     try:
    #         os.mkdir(path)
    #         os.chdir(path)
            
            
    #     except Exception as e:
    #         os.chdir(path)
            
    #         if xtra:
    #             filename= str(self.partition_count) + '.txt'
    #             print(filename)
    #             f=open(filename,'a')
    #             seek=f.tell()
    #             #print(seek)
    #             f.write(content[:(threshold-xtra)])
                
    #             f.close()
            
    #             content=content[(threshold-xtra):]
        
                
    #     finally:
            
    #         rem = self.length % threshold
    #         x=1 if rem>0 else 0
    #         prev_count=self.partition_count
    #         self.partition_count = self.length//threshold + x
            
    #         seek=0
    #         for i in range(prev_count+1,self.partition_count):
    #             filename= str(i) + '.txt'
    #             print(filename)
    #             f=open(filename,'a')
    #             f.write(content[seek:seek+threshold])
    #             seek += threshold
    #             f.close()
    #         if rem:
    #             filename= str(self.partition_count) + '.txt'
    #             f=open(filename,'a')
    #             print(filename)
    #             f.write(content[seek:])
                
    #             f.close()
    #         os.chdir(parent)

