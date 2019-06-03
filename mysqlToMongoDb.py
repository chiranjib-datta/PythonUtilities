import mysql.connector
import math;
import pymongo;
import logging, sys;
import datetime, os ,json;
import datetime;
import pymongo;
from pymongo import MongoClient;
from collections import Set, MutableSequence


def startMigrate(mongoDb,mySqlShema,mycursor):
  logger.info("Started migration");
  
  mycursor.execute("SHOW tables")
  myresult = mycursor.fetchall();
  print("got tables");
  columns=[]
  for x in myresult:
    print("Started for "+str(x));
    x=str(x);
    x=x.split("'")[1]
    countQuery="SELECT count(*) FROM "+mySqlShema+"."+str(x);
    mongoCol=mongoDb[str(x)];
    mycursor.execute(countQuery);
    totalCount=mycursor.fetchone()[0];
    perPage=100;
    totalPage=int(math.ceil(totalCount/perPage));
    currentPage=1;
    start=0;
    for page in range(0, totalPage):
      end=(page+1)*perPage-1;
      if end>totalCount:
        end=totalCount;
      selectQuery="SELECT * FROM "+mySqlShema+"."+str(x)+" LIMIT "+str(start)+" , "+str(perPage)
      mycursor.execute(selectQuery);   
      columns = [col[0] for col in mycursor.description]
      rows = [dict(zip(columns, row)) for row in mycursor.fetchall()]        
      for row in rows:
        json=row;
        for key, val in row.items():
          if type(val) is set:
              json[key]=list(val);
          elif val is not None:
                json[key]=val;          
          else:
              json[key]='';
        mongoCol.insert(json);
      
      start=end+1;
    print("Ended for "+str(x));
    
  logger.info("Finished migration");
  
    

if __name__=="__main__":
  logfile = "sqltoMongo.log"     
  logging.basicConfig(filename=logfile,format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S');
  logger = logging.getLogger(__name__)
  dataToLog = "Script Started"
  logger.info(dataToLog);
  
  mongodbUrl="mongodb://localhost:27017,localhost:27018,localhost:27019";
  mongoDataBaseName="test"
  
  mySqlHost="localhost";
  mySqlUser="root";
  mySqlPassword="root";
  mySqlShema="test";
  
  try:
    conn = MongoClient(mongodbUrl)
    logger.info("MongoDb Connected successfully!!!");
    # database
    mongoDb = conn[mongoDataBaseName]
    
    mydb = mysql.connector.connect(
      host=mySqlHost,
      user=mySqlUser,
      passwd=mySqlPassword,
      database=mySqlShema
    );
    
    mycursor = mydb.cursor(buffered=True);
    
    startMigrate(mongoDb,mySqlShema,mycursor);
    mycursor.close();
    startAssertDumpReference(shackbuilder);
    logger.info('\n###################################################################\n');
    print("Finished Migration");
    
  except Exception as e:
    logger.error(e);
    logger.error("Error from the script.Unexpected exiting");

  
  
  


  
  