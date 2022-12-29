import os
import pymysql.cursors
path_wx_data = "/Users/bharath/Desktop/Stuff/Personal Projects/CodeChallenge/code-challenge-template/wx_data"
path_yld_data = "/Users/bharath/Desktop/Stuff/Personal Projects/CodeChallenge/code-challenge-template/yld_data"


conn = pymysql.connect(host='localhost',
                       port = 3306,
                       user='root',
                       password='password',
                       db='code',
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor)



def storedata(file_path,filename):
    insert_querry_wx_data='INSERT INTO wx_data(station,date,maxtemp,mintemp,precipitation) VALUES(%s,%s,%s,%s,%s)'

    file = open(file_path, 'r')
    for line in file:
        temp=line[:len(line)-1].split("\t")
        cursor.execute(insert_querry_wx_data,(filename[:len(filename)-4],temp[0],temp[1],temp[2],temp[3]))

cursor = conn.cursor()

def wxdatastore(path_wx_data):
    for file in os.listdir():
        if file.endswith(".txt"):
            file_path = f"{path_wx_data}/{file}"
        
                # call read text file function
            storedata(file_path,file)
            conn.commit()

def storeylddata(file_path,filename):
    insert_querry_yld_data='INSERT INTO ylddata(year,yeild)VALUES(%s,%s)'
    file = open('US_corn_grain_yield.txt', 'r')
    for line in file:
        temp=line[:len(line)-1].split("\t")
        cursor.execute(insert_querry_yld_data,(temp[0],temp[1]))



def ylddatastore(path_yld_data):
    for file in os.listdir():
        if file.endswith(".txt"):
            file_path = f"{path_wx_data}/{file}"
        
                # call read text file function
            print(file)
            storeylddata(file_path,file)
            conn.commit()




os.chdir(path_wx_data)

wxdatastore(path_wx_data)  

os.chdir(path_yld_data)
ylddatastore(path_yld_data)


