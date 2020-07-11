import requests

import time
import pandas as pd
import json
import sqlalchemy as sq

def main():
    try:
        db=pd.read_csv('dbconfig.csv')
        for line,row in db.iterrows():
            username=row['username']
            password=row['password']
            server=row['server']
            port=row['port']
            dbname=row['dbname']
        
        engine=sq.create_engine('postgresql://'+str(username)+':'+str(password)+'@'+str(server)+':'+str(port)+'/'+str(dbname)+'')
        
        con=engine.connect()
        query='select * from apiconfig where id=2'
        auth=pd.read_sql(query,engine)
        for i,j in auth.iterrows():
            user=str(j['username'])
            pwd=str(j['pwd'])
            endpoint=str(j['endpoint'])
        truncquery='truncate table public.catalog_series'
        con.execute(truncquery)
        top=5000
        skip=0
        
        
        while(1==1):
            url=''+str(endpoint)+'?$top='+str(top)+'&$skip='+str(skip)+''
            col='SeriesID,SeriesName,SeriesNameTranslated,SeriesNameCode,UnitID,Scale,LastModified,Active'
            col=col.split(',')
            res=requests.get(url,auth=(user,pwd))
           
            print(url)
            data=res.json()
            
            result=data['value']
            
            if len(result)>0:
      
                dv_data=json.dumps(result)
                df=pd.read_json(dv_data)
                print(df)
                for i,j in df.iterrows():
                    for p in col:
                        if p not in j:
                            
                            df[p]='null'
                df[col].to_sql('catalog_series',engine,index=False,if_exists='append')
            else:
                break
                            
                
                
            skip=skip+top
    except Exception as e:
        print(e)
    


if __name__=='__main__':

    main()
