import os

def get_topic_offsets(WORKING_DIRECTORY):
    os.chdir(WORKING_DIRECTORY+'/broker1')
    all_topics=os.listdir()
    all_topics.remove('log.txt')

    last_seek={}
    for i in all_topics:
        os.chdir(WORKING_DIRECTORY+'/broker1/'+i)
        all_files=os.listdir()
        length=len(all_files)
        last_file=all_files[length-1]
        f=open(last_file,'r')
        data = f.read()
        number_of_characters = len(data)
        last_seek[i] = (last_file,number_of_characters)
    
    os.chdir(WORKING_DIRECTORY)
    
    return last_seek

