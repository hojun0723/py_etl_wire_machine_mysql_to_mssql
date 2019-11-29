#MYSQL 설비데이터 -> DW 적재

#import sqlite3
import pyodbc
#import MySQLdb
#import mysqlclient
import datetime
# import threading
import sys
import time

def MariaDBtoMSSQL():

    try:

        # SOURCE SERVER INFO
        cnxn = pyodbc.connect("DRIVER={MySQL ODBC 3.51 Driver}; SERVER=123.123.123.123;DATABASE=database; UID=userid; PASSWORD=password;") 
        cursor = cnxn.cursor()

        # TARGET SERVER INFO
        cnxn2 = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=123.123.123.123;DATABASE=database;UID=userid;PWD=password')
        cursor2 = cnxn2.cursor()
		
        # SQL
        # table  = "QLT_JAJOO"
        # ilja   = "2017-05-15"
        # addsql = " WHERE ILJA < '" + ilja + "'"
        # sql    = "SELECT * FROM " + table + addsql

        '''
        cursor.execute("""\
        SELECT PJI.*, PJO.*, 
                CST.ABCGS 
        FROM  dbo.Traverse AS TRE 
                      LEFT OUTER JOIN dbo.TraversePreEntry AS TPE 
                            ON TRE.JobNum = dbo.GetJobNumberFromGroupId(TPE.GroupId)
                      LEFT OUTER JOIN AutoCADProjectInformation AS PJI
                            ON TRE.JobNum = PJI.JobNumber
                      LEFT OUTER JOIN CalculationStorageReplacement AS CST
                            ON CST.ProjectNumber = dbo.GetJobNumberFromGroupId(TPE.GroupId)
                      LEFT OUTER JOIN dbo.TraverseElevations AS TEV
                          ON TRE.TraverseId = TEV.TraverseId
                      LEFT OUTER JOIN VGSDB.dbo.ProjectOffice PJO
                            ON PJI.PjbId = PJO.PjbId
        where jobnum = 1205992""")
        '''

        '''
        changelog
        errorldc
        joborder

        logininfo
        measurement
        pcinfo
        prd010
        producti
        producti_sum
        producti_sum_n
        qualityparm
        statisticdata
        terminalqdc
        '''

        tableGroup = ['changelog', 'errorldc', 'joborder', 'logininfo', 'measurement', 'pcinfo', 'prd010', 'producti', 'producti_sum', 'producti_sum_n', 'qualityparm', 'statisticdata', 'terminalqdc']
        #tableGroup = ['pcinfo']

        for tablename in tableGroup:

            #tablename = 'joborder'

            sql = ""
            sql = sql + """\
            select * from 
            
            """
            sql = sql + tablename

            print(tablename)

            msg = "\nselect start: " + str(datetime.datetime.now())
            print(msg)

            r = cursor.execute(sql)
                   
            c = [column[0] for column in r.description]

            results = []
            
            k = 0

            for row in cursor.fetchall():

                k += 1
                results.append(dict(zip(c, row)))

                #print(row)

            msg = "%s row(s)" % (str(k))
            print(msg)

            msg = "select end  : " + str(datetime.datetime.now())
            print(msg)
            
            msg = "\ninsert start: " + str(datetime.datetime.now())
            print(msg)

            k = 0
            for myDict in results:
                
                k += 1
                columns = ','.join(myDict.keys())

                # %s 인 경우에는 에러 발생하여 ? 로 placeholders 변경
                placeholders = ','.join(['?'] * len(myDict))
                #print(placeholders)

                sql = "insert into " + 'new_table' + tablename + " (%s) values (%s)" % (columns, placeholders)

                #print(sql)
                #return

                #print(sql)
                #print(list(myDict.values()))
                #return
                
                cursor2.execute(sql, list(myDict.values()))
                
                #sys.stdout.write("#")
                #sys.stdout.flush()

                msg = "%s row(s)" % (str(k))
                print(msg, end='')
                print("\r", end='')

                
            msg = "%s row(s)" % (str(k))
            print(msg)

            msg = "insert end  : " + str(datetime.datetime.now())
            print(msg)
            
            cnxn2.commit()

            time.sleep(5)
        
    except Exception as e:
        print('error:', e)

MariaDBtoMSSQL()
