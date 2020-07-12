import requests
import time
import pandas as pd
import json
import sqlalchemy as sq

from util import (
    get_api_configurations,
    get_data_from_dataabse_for_sql,
)


def load_dimension_data(dimension_name, table_name):
    try:
        username, password, api_endpoint = get_api_configurations()

        truncate_query = f"truncate table {table_name}"
        df_dummy = get_data_from_dataabse_for_sql(truncate_query)

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
                    for p in col:
                        if p not in j:
                            
                            df[p]='null'
                
                            
                
                df[col].to_sql('catalog_areas',engine,index=False,if_exists='append')
            else:
                break

            skip=skip+top
        
    except Exception as e:
        print(e)
    

    

if __name__=='__main__':

    main()
