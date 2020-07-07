import requests
import sys
import time
import pandas as pd
import json
import logging
import sqlalchemy as sq
import datetime
import math

def main():
    try:
        ddate=datetime.datetime.now().strftime("%Y-%m-%d")
        logf='runlog_Value_Weekly_'+str(ddate)+'.log'
        logger=logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        fh=logging.FileHandler(logf)
        fh.setLevel(logging.INFO)
        formatter=logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        engine=sq.create_engine('postgresql://postgres:abhi#789@localhost:4444/postgres')
        con=engine.connect()
        query='select * from authentication'
        auth=pd.read_sql(query,engine)
        for i,j in auth.iterrows():

            user=str(j['username'])
            pwd=str(j['pwd'])
         
        query='select * from execonfig where "isActive"=1 order by "rowid"'
        configdf=pd.read_sql(query,engine)
        if configdf.empty:
            print("No Active record to read")
        for p,q in configdf.iterrows():
            seriesid=q['SeriesID']
            rowid=q['rowid']
            areaid=q['AreaID']
            lastmodified=q['LastModified']
            
            nlastmodified=lastmodified
            if lastmodified is None or lastmodified is pd.NaT:
                lastmodified='2014-01-01 00:00:00'

            print('*************************************')
            logger.info('*************************************')
            logger.info('Executing the code for RecordID '+str(rowid)+' and SeriesID '+str(seriesid)+'and LastModifiedDate '+str(lastmodified))
            print('Executing the code for RecordID '+str(rowid)+' and SeriesID '+str(seriesid)+'and LastModifiedDate '+str(lastmodified))
            date=str(lastmodified).replace(' ','T')
            start=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            top=2000
            skip=0
            cnt=0
            num=0
            while(1==1):
                print("=====================")
                logger.info("=====================")
                print('Starting to read records for RecordID '+str(rowid)+' and SeriesID '+str(seriesid)+' for Batch '+str(num))
                logger.info('Starting to read records for RecordID '+str(rowid)+' and SeriesID '+str(seriesid)+' for Batch '+str(num))

                if areaid is None or math.isnan(areaid):
                    
                    url='http://data.commoditymarkets.com/SoleraAdvisors/Value_Weekly?$filter=SeriesID eq '+str(seriesid)+' AND LastModified gt '+str(date)+'Z &$top='+str(top)+'&$skip='+str(skip)+'& $format=json'
                
                else:
                    
                    url='http://data.commoditymarkets.com/SoleraAdvisors/Value_Weekly?$filter=AreaID eq '+str(areaid)+' AND SeriesID eq '+str(seriesid)+'AND LastModified gt '+str(date)+'Z &$top='+str(top)+'&$skip='+str(skip)+'& $format=json'

               
                col='RecordID,ProductID,SeriesID,AreaID,ReportPeriod,CollectionDate,WeekEnding,High,Low,Value,SourceID,LastModified,Active'
                col=col.split(',')
                res=requests.get(url,auth=(user,pwd))
               
                print(url)
                logger.info(url)
                data=res.json()
                result=data['value']
                
                if (len(result)>0):
                    uquery='update execonfig set "LastRunDate"="StartTime","LastRunStatus"="Status","Status"=''\'Running\''',"StartTime"= \''+str(start)+'\' where "rowid"= '+str(rowid)
                    
                    con.execute(uquery)

                    dv_data=json.dumps(result)

                    cnt=int(cnt)+int(len(df.index))
                    print('Batch '+str(num)+' read count '+str(len(df.index)))
                    logger.info('Batch '+str(num)+' read count '+str(len(df.index)))
                    for i,j in df.iterrows():
                        for p in col:
                            if p not in j:
                                
                                df[p]=None
                    for i,j in df.iterrows():
                        
                        RecordID=j['RecordID']
                        ProductID=j['ProductID']
                        SeriesID=j['SeriesID']
                        AreaID=j['AreaID']
                        ReportPeriod=j['ReportPeriod']
                        CollectionDate=j['CollectionDate']
                        WeekEnding=j['WeekEnding']
                        High=j['High']
                        Low=j['Low']
                        Value=j['Value']
                        SourceID=j['SourceID']
                        LastModified=j['LastModified']
                        Active=j['Active']
                        

                        if math.isnan(High): 
                            High='NULL'
                        if math.isnan(Low):
                            Low='NULL'


                        query1='''INSERT INTO public.value_weekly(
                        "RecordID", "ProductID", "SeriesID", "AreaID", "ReportPeriod", "WeekEnding", "High", "Low", "Value", "CollectionDate", "SourceID", "LastModified", "Active")'''
                        if High =='NULL' and Low =='NULL':
                            query2='''VALUES (\''''+str(RecordID)+'\''',\''''+str(ProductID)+'\''',\''''+str(SeriesID)+'\''',\''''+str(AreaID)+'\''',\''''+str(ReportPeriod)+'\''',\''''+str(WeekEnding)+'\''','''+str(High)+''','''+str(Low)+''',\''''+str(Value)+'\''',\''''+str(CollectionDate)+'\''',\''''+str(SourceID)+'\''',\''''+str(LastModified)+'\''',\''''+str(Active)+'\''')'''
                        else:
                            query2='''VALUES (\''''+str(RecordID)+'\''',\''''+str(ProductID)+'\''',\''''+str(SeriesID)+'\''',\''''+str(AreaID)+'\''',\''''+str(ReportPeriod)+'\''',\''''+str(WeekEnding)+'\''',\''''+str(High)+'\''',\''''+str(Low)+'\''',''\''''+str(Value)+'\''',\''''+str(CollectionDate)+'\''',\''''+str(SourceID)+'\''',\''''+str(LastModified)+'\''',\''''+str(Active)+'\''')'''
                        query3='''on conflict ("RecordID","SeriesID","AreaID","ReportPeriod")
                        Do
                            Update
                            set "ProductID" = EXCLUDED."ProductID" , "WeekEnding"=EXCLUDED."WeekEnding","High"=EXCLUDED."High","Low"=EXCLUDED."Low","Value"=EXCLUDED."Value","CollectionDate"=EXCLUDED."CollectionDate","SourceID"=EXCLUDED."SourceID","LastModified"=EXCLUDED."LastModified","Active"=EXCLUDED."Active";'''

                

                        query=query1+query2+query3
                        

                        con.execute(query)
                    query='select max("LastModified")  from public.value_weekly where "RecordID"=\''+str(RecordID)+'\' and "SeriesID"=\''+str(SeriesID)+'\'and "ReportPeriod"=\''+str(ReportPeriod)+'\' and "AreaID"=\''+str(AreaID)+'\''
                    
                    udf=pd.read_sql(query,engine)
                    for k,l in udf.iterrows():
                        nlastmodified=j['LastModified']
                        print(nlastmodified)
                    

                else:

                    print('Read complete for record '+str(rowid)+' and SeriesID '+str(seriesid))
                    print('Total record read '+str(cnt))
                    logger.info('Total record read '+str(cnt))
                    print("=====================")
                    time.sleep(10)
                    logger.info('Read complete for record '+str(rowid)+' and SeriesID '+str(seriesid))
                    logger.info("=====================")
                    
                    break
                skip=skip+top
                num=num+1

            end=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            uquery='update execonfig set "Status"=''\'Successful\''',"isActive"=1,"EndTime"= \''+str(end)+'\',"LastModified"= \''+str(nlastmodified)+'\' where "rowid"= '+str(rowid)

            con.execute(uquery)    
            
        
    except Exception as e:
        print('===Error Details===')
        print(e)
        end=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        uquery='update execonfig set "Status"=''\'Failed\''',"EndTime"= \''+str(end)+'\' where "rowid"= '+str(rowid)
        
        con.execute(uquery)
        tb=sys.exc_info()[2]
        print("An error occured on line"+str(tb.tb_lineno))
        
    


if __name__=='__main__':

    main()
