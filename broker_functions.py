import os
def consume(self,topic_name,beg):
    os.chdir(WORKING_DIRECTORY+'/broker1/'+topic_name)
    all_files=os.listdir()
    ret=''
    offset=0
    if beg!='--from-beginning':
        index = all_files.index(Topic_cons.instances[self.id][topic_name][0])
        all_files=all_files[index:]
        offset = Topic_cons.instances[self.id][topic_name][1]
    
    off_flag=0
    for i in all_files:
            
        f=open(i,'r')
        if off_flag==0: 
            f.seek(offset)
            off_flag=1
        
        ret+=f.read()
        f.close()
    f=open(i,'r')
    data=f.read()
    Topic_cons.instances[self.id][topic_name] = (i,len(data))
    f.close()
    os.chdir(WORKING_DIRECTORY+'/broker1')
    f=open('log.txt','a')
    f.write(str(Topic_cons.instances)+'\n')
    os.chdir(WORKING_DIRECTORY)
    
    return ret

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
                f=open(filename,'a')
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
                f=open(filename,'a')
                f.write(content[seek:seek+threshold])
                seek += threshold
                f.close()
            if rem:
                filename= str(self.partition_count) + '.txt'
                f=open(filename,'a')
                # print(filename)
                f.write(content[seek:])
                
                f.close()
            os.chdir(parent)
        f=open('log.txt','a')
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

