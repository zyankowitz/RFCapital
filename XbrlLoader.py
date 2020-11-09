from os import listdir
from os.path import isfile, join
import xml.etree.ElementTree as ET
import mysql.connector

# filename = 'c:/Users/zivy/source/repos/datacall/'
filename = 'c:/Users/zivy/source/repos/dataubpr/'
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Panda230",
  database="rf2capital",
  auth_plugin='mysql_native_password'
)

def insertRecord(id,org_id,period,column,amount):
    mycursor = mydb.cursor()
    sql = "INSERT INTO UBPR (ID,ORG_ID, PERIOD ,"+column+") VALUES (%s, %s,%s,%s)"
    val = (id,org_id,period,amount)
    mycursor.execute(sql, val)

    mydb.commit()

def updateRecord(id,column,amount):
    mycursor = mydb.cursor()
    sql = "update UBPR set "+column+"='"+amount+"' where id="+str(id)
    mycursor.execute(sql)
    mydb.commit()

def getOrgRecordId(org_id,period):
    mycursor = mydb.cursor()
    sql = "select id from UBPR where org_id=" +org_id +" and period = '" +period +"'"
    mycursor.execute(sql)
    myresult = mycursor.fetchone()
    if myresult==None:
        return 0
    return myresult[0]

def getMaxId():
    mycursor = mydb.cursor()
    sql = "select max(id) from UBPR"
    mycursor.execute(sql)
    myresult = mycursor.fetchone()
    if myresult[0]==None:
        return 0
    return myresult[0]

columnList=[]
onlyfiles = [f1 for f1 in listdir(filename) if isfile(join(filename, f1))]    


def getTotalAssets():
    maxId = getMaxId()
    if maxId ==None:
        id=1
    else:
        id = int(getMaxId())        
    for item in root.iter(nameSpace+'UBPR2170'):
        amount = item.text
        period = item.attrib.get('contextRef')[11:]
        entityId = item.attrib.get('contextRef')[3:10]
        id+=1
        # insertRecord(id,entityId,period,amount)
        # print(item)
        return entityId

def getTagList():
    f = open("c:/Users/zivy/source/repos/db/dbload.csv", "r")
    for x in f:
        indexOfComma = x.find(',')
        title = x[1:indexOfComma]
        column = x[indexOfComma+1:len(x)-1]
        columnList.append(column)
       

def getUBPRTag(tag):
    found = False
    for item in root.iter(nameSpace1+tag):
        amount = item.text
        period = item.attrib.get('contextRef')
        indexOfPeriod= period.find('2019-12-31')
        if indexOfPeriod>0:
            period = period[indexOfPeriod:]
        if period=='2019-12-31':
            found = True
            entityId = item.attrib.get('contextRef')[3:indexOfPeriod-1]
            id=getOrgRecordId(entityId,period)
            if id>0:
                updateRecord(id,tag,amount)
            else:
                id = int(getMaxId()) 
                insertRecord(id+1,entityId,period,tag,amount)
    # if found == False:
    #     print(nameSpace1+tag + " , couldnt be found")
    for item in root.iter(nameSpace2+tag):
        amount = item.text
        period = item.attrib.get('contextRef')
        indexOfPeriod= period.find('2019-12-31')
        if indexOfPeriod>0:
            period = period[indexOfPeriod:]
        if period=='2019-12-31':
            found = True
            entityId = item.attrib.get('contextRef')[3:indexOfPeriod-1]
            id=getOrgRecordId(entityId,period)
            if id>0:
                updateRecord(id,tag,amount)
            else:
                id = int(getMaxId()) 
                insertRecord(id+1,entityId,period,tag,amount)
    for item in root.iter(nameSpace3+tag):
        amount = item.text
        period = item.attrib.get('contextRef')
        indexOfPeriod= period.find('2019-12-31')
        if indexOfPeriod>0:
            period = period[indexOfPeriod:]
        if period=='2019-12-31':
            found = True
            entityId = item.attrib.get('contextRef')[3:indexOfPeriod-1]
            id=getOrgRecordId(entityId,period)
            if id>0:
                updateRecord(id,tag,amount)
            else:
                id = int(getMaxId()) 
                insertRecord(id+1,entityId,period,tag,amount)
            # print(item)
    # if found == False:
    #     print(nameSpace1+tag + " , couldnt be found")
    return found

fileCount=0
getTagList()

for file in onlyfiles: 
    found = False
    # if fileCount>2:
    #     break;
    if file.find('100236')>0:
        print('asd')
    tree = ET.parse(filename+file)
    root = tree.getroot()
    nameSpace1 = '{http://www.cdr.ffiec.gov/xbrl/ubpr/v113/SourceConcepts}'
    nameSpace2 = '{http://www.cdr.ffiec.gov/xbrl/ubpr/v113/Concepts}'
    nameSpace3='{http://www.ffiec.gov/xbrl/call/concepts}'
    # getTotalAssets()
    for tag in columnList:
        if getUBPRTag(tag) == True:
            found=True
    if found==False:
        print(file)
        print("couldnt find any item for file: " + file)
    fileCount+=1
    # for child in root:
    #     if child.tag.find('TierOneRiskBasedCapital')>0:
    #         print(child.tag, child.attrib)