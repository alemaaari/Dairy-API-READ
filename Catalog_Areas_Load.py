import requests
import time
import pandas as pd
import json
import sys
import sqlalchemy as sq

from util import (
    get_api_configurations,
    push_to_database,
    insert_data_to_database
)


def load_dimension_data(username, password, api_endpoint,tablename):
    try:
        

        truncate_query = f"truncate table {tablename}"
        
        push_to_database(truncate_query)

        top = 5000
        skip = 0

        while 1 == 1:

            url = ''+str(api_endpoint)+'?$top='+str(top)+'&$skip='+str(skip)+''
            col = 'AreaID,AreaName,AreaNameTranslated,AreaNameCode,AreaDescription,LastModified,Active'
            col = col.split(',')
            res = requests.get(url, auth=(username, password))
           
            print(url)
            data=res.json()
            result=data['value']
           
            if len(result)>0:
                dv_data=json.dumps(result)
                df=pd.read_json(dv_data)
                print(df)
                for i,j in df.iterrows():
                    for colname in col:
                        if colname not in j:
                            
                            df[colname]='null'
                
                            

                insert_data_to_database(df[col],tablename,'append')
            else:
                break

            skip=skip+top
        
    except Exception as e:
        print(e)
        tb=sys.exc_info()[2]
        print("An error occured on line"+str(tb.tb_lineno))
    
def main():
    query='select * from apiconfig where id=1'
    username, password, api_endpoint,tablename = get_api_configurations(query)
    load_dimension_data(username, password, api_endpoint,tablename)

if __name__=='__main__':

    main()
