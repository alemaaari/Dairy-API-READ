import requests

import time
import pandas as pd
import json
import sqlalchemy as sq

def main():
    try:
        engine=sq.create_engine('postgresql://postgres:abhi#789@localhost:4444/postgres')
        con=engine.connect()
        query='select * from authentication'
        auth=pd.read_sql(query,engine)
        for i,j in auth.iterrows():
            user=str(j['username'])
            pwd=str(j['pwd'])
         
        truncquery='truncate table public.catalog_sources'
        con.execute(truncquery)
        top=5000
        skip=0
        while(1==1):
            url='http://udmdirect.dairymarkets.com/Universal/UDM_Catalog_Sources?$top='+str(top)+'&$skip='+str(skip)+''
            col='SourceID,ReportID,SourceName,LastModified,Active'
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
                
                df[col].to_sql('catalog_sources',engine,index=False,if_exists='append')
            else:
                break
            skip=skip+top
        
    except Exception as e:
        print(e)
    


if __name__=='__main__':

    main()
