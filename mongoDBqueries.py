import pymongo

from ip_lookup import lookup_ip
from datetime import datetime
import time
import pandas as pd
pymongo_uri='mongodb+srv://lucbellintani:190897@cluster0.kqtvw.mongodb.net/?retryWrites=true&w=majority'

client = pymongo.MongoClient(pymongo_uri)
db = client['UniqueVisits']

def checkAccountExists(accountNumber:int) -> bool:
    col=db['Accounts']

    num_accounts=col.count_documents({'account_id':accountNumber})

    if num_accounts==0:
        return False
    elif num_accounts==1:
        return True
    else:
        return False

def checkIPExists(ip_address:str,account_number:int) -> bool:
    col=db['Visits'+'-'+str(account_number)]

    num_accounts=col.count_documents({'ip_address':ip_address})

    if num_accounts==0:
        return False
    else:
        return True

def check_id_exists(id_code:str,account_number:int) -> bool:
    col=db['Visits'+'-'+str(account_number)]

    visits_old_code=col.count_documents({'id_code_old':id_code})

    if (visits_old_code>0):
        return True
    else:
        return False

def write_new_to_db(ip_address,code_new,page_url,account_number:int,session_id:str,code_old=None,isFirst=False,wasReferred=None,code_reused=False):
    col=db['Visits'+'-'+str(account_number)]
    now = datetime.now()
    now=time.mktime(now.timetuple())

    insDict={
        'ip_address':ip_address,
        'id_code_old':code_old,
        'id_code_new':code_new,
        'entry_time_unix':now,
        'entry_time':datetime.now(),
        'page_url':page_url,
        'ip_info':lookup_ip(ip_address=ip_address),
        'post_processed':False,
        'session_id':session_id
    }
    if isFirst:
        insDict['isFirst']=True
    else:
        insDict['isFirst']=False

    if wasReferred:
        insDict['wasReferred']=True
    else:
        pass

    if code_reused:
        insDict['code_reused']=True
    else:
        insDict['code_reused']=False
    
    col.insert_one(insDict)

def get_most_used_codes(account_number:int,count_return=5,return_count_dict=False,time_window='2w',post_process_only=True,first_only=True):
    col=db['Visits-'+str(account_number)]
    
    #Time stuff
    time_window_list=list(time_window)
    time_unit=time_window_list[-1]
    
    seconds_per_time_unit={'s':1,'m':60,'h':3600,'d':86400,'w':604800,'M':16394400,'y':31536000}
    
    now=time.time()
    
    if time_unit in ['s','m','h','d','w','M','y']:
        try:
            time_period=float(''.join(time_window_list[:-1]))
        except:
            raise ValueError('Time units incorrect. Expected int/float. Got {}'.format(''.join(time_window_list[:-1])))
        time_filter=now-time_period*seconds_per_time_unit[time_unit]
    elif time_unit==None:
        time_filter=None
    else:
        raise ValueError('Time unit was not one of; s, m, h, d, w, M, y. Instead got {}'.format(time_unit))
        
    filter_query={}
    if time_unit:
        filter_query['entry_time_unix']={ '$gt': time_filter }
    else:
        pass
    
    if post_process_only:
        filter_query['post_processed']=post_process_only
    else:
        pass
    
    if first_only:
        filter_query['isFirst']=first_only
    else:
        pass
    
    cur=col.find(filter_query).distinct('id_code_new')
    unique_share_codes=list(cur)
    
    cur2=col.find(filter_query)
    
    df=pd.DataFrame(cur2)
    df2=df[df.id_code_old.isin(unique_share_codes)]
    
    df2=df2.groupby(['id_code_old'])['ip_address'] \
        .count() \
        .reset_index() \
        .sort_values(by='ip_address',ascending=False) \
        .head(count_return)
    
    df2=df2[df2.ip_address>1]
    
    most_used_codes_dict= dict(zip(df2['id_code_old'],df2['ip_address']))
    
    if return_count_dict:
    
        return most_used_codes_dict
    else:
        return list(most_used_codes_dict.keys())