import mysql.connector

f = open("c:/Users/zivy/source/repos/db/dbload.csv", "r")

updateMapping = "insert into MAPPING_KEY (UBPRKEY,UBPR_DESC) VALUES(%s,%s)"

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Panda230",
  database="rf2capital",
  auth_plugin='mysql_native_password'
)

def updateMappingTable(UBPRKEY,UBPRDESC):
    mycursor = mydb.cursor()
    val = (UBPRKEY,UBPRDESC)
    mycursor.execute(updateMapping, val)

    mydb.commit()

def createCsv(UBPRKEY,UBPRDESC):
    f = open("c:/Users/zivy/Downloads/dbload.csv", "w")
    mycursor = mydb.cursor()
    mycursor2 = mydb.cursor()
    sql = "select * from UBPR"
    sql2 = "select UBPR_DESC from MAPPING_KEY where UBPRKEY="
    # sql2 = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'rf2capital' AND TABLE_NAME = 'UBPR';
    mycursor.execute(sql2)
    myresult = mycursor.fetch()
    for res in myresult:
      mycursor2.execute(sql2+res)
      myresult2 = mycursor2.fetchone()
      f.write(myresult2[0]+",")
    # mycursor.execute(updateMapping, val)

    # mydb.commit()


def createDataTable():
  singleRow = " TEXT DEFAULT NULL,"
  createTable = "CREATE TABLE UBPR(ID bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
  createTable+="ORG_ID varchar(255) DEFAULT NULL,"
  createTable+="PERIOD varchar(255) DEFAULT NULL,"

  for x in f:
    indexOfComma = x.find(',')
    title = x[1:indexOfComma]
    column = x[indexOfComma+1:len(x)-1]
    createTable+=column + singleRow
  createTable+="PRIMARY KEY (`ID`)) "
  print(createTable)

def updateMappingDesc():
  for x in f:
    indexOfComma = x.find(',')
    desc = x[1:indexOfComma]
    key = x[indexOfComma+1:len(x)-1]
    updateMappingTable(key,desc)

# updateMappingDesc()
createDataTable()
