import logging 
logger=logging.getLogger() 
logger.setLevel(logging.DEBUG)
from difflib import SequenceMatcher
import pandas as pd
import logging
import json
from db_utils import DB


import os
os.environ['HOST_IP'] = '35.173.139.208'
os.environ['LOCAL_DB_USER'] = 'root'
os.environ['LOCAL_DB_PASSWORD'] = 'AlgoTeam123'
os.environ['LOCAL_DB_PORT'] = '3306'

tenant_id = 'deloitte.acelive.ai'

db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}



def partial_match(input_string, matchable_strings, threshold=75):
    """Returns the most similar string to the input_string from a list of strings.
    Args:
        input_string (str) -> the string which we have to compare.
        matchable_strings (list[str]) -> the list of strings from which we have to choose the most similar input_string.
        threshold (float) -> the threshold which the input_string should be similar to a string in matchable_strings.
    Example:
        sat = partial_match('lucif',['chandler','Lucifer','ross geller'])"""
    
    logging.info(f"input_string is {input_string}")
    logging.info(f"matchable_strings got are {matchable_strings}")
    result = {}
    words = matchable_strings
    match_word = input_string
    logging.info(f"words got for checking match are : {words}")
    max_ratio = 0
    match_got = ""
    for word in words:
        try:
            ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
            if ratio > 75 and ratio > max_ratio:
                max_ratio = ratio
                match_got = word
                print(match_got)
        except Exception as e:
            logging.error("cannnot find match")
            logging.error(e)
            result['flag'] = 'False'
            result['data'] = {'reason':'got wrong input for partial match','error_msg':str(e)}
            return result
    if match_got:
        logging.info(f"match is {match_got} and ratio is {max_ratio}")
        result['flag'] = 'True'
        result['data'] = {'value':match_got}
    else:
        logging.info(f"match is {match_got} and ratio is {max_ratio}")
        result['flag'] = 'False'
        result['data'] = {'reason':f'no string is partial match morethan {threshold}%','error_msg':'got empty result'}
    return result

def date_transform(date, input_format='dd-mm-yyyy', output_format='dd-mm-yyyy'):
    """Date format change util function
    Args:
        date (str) -> the date string which needs to be converted
        input_format (str) -> the input format in which the given date string is present.
        output_format (str) -> the output format to which we have to convert the data into.
    Returns:
        result (dict)
        result is a dict having keys flag, data.Depending on the flag the data changes.
        
        if flag is True: data contains the key value and value is the converted date.
        if flag is False: data contains the error_msg say why its failed.
    Example:
        x = date_transform('23-03-2020','dd-mmm-yyyy','dd-mm-yy')"""
    
    logging.info(f"got input date is : {date}")
    logging.info(f"got input format is : {input_format}")
    logging.info(f"got expecting output format is : {output_format}")
    result = {}
    date_format_mapping = {'dd-mm-yyyy':'%d-%m-%Y','dd-mmm-yyyy':'%d-%b-%Y','dd-MMMM-yyyy':'%d-%B-%Y','dd-mm-yyyy':'%d-%m-%Y','dd-MMM-yy':'%d-%b-%y','dd-mmmm-yy':'%d-%B-%y','mmm-dd-yyyy':'%d-%m-%Y','yyyy-MMM-dd':'%Y-%b-%d','yy-dd-mmm':'%y-%d-%b','mmm-dd-yy':'%b-%d-%y','yy-mmm-dd':'%y-%b-%d','yy-mm-dd':'%y-%m-%d','dd-yyyy-mmmm':'%d-%Y-%B','dd-mm-yy':'%d-%m-%y','mm-dd-yy':'%m-%d-%y','yy-dd-mm':'%y-%d-%m','mm-dd-yyyy':'%m-%d-%Y','yyyy-dd-mm':'%Y-%d-%m','yyyy-mm-dd':'%Y-%m-%d','mmmm-dd-yy':'%B-%d-%y','yy-dd-mmmm':'%y-%d-%B','yy-mmmm-dd':'%y-%B-%d','mmmm-dd-yyyy':'%B-%d-%Y','yyyy-dd-mmmm':'%Y-%d-%B','yyyy-mmmm-dd':'%Y-%B-%d','yyyy-dd-mmm':'%Y-%d-%b'}
    try:
        input_format_ = date_format_mapping[input_format]
        output_format_ = date_format_mapping[output_format]
    except:
        input_format_ = '%d-%m-%Y'
        output_format_ = '%d-%m-%Y'
        
    try:
        date_series = pd.Series(date)
        logging.info(f"got series is : {date_series}")
    except Exception as e:
        logging.error("cannnot convert given input to pandas series")
        logging.error(e)
        result['flag'] = False
        result['data'] = {'reason':'cannnot convert given input to pandas series','error_msg':str(e)}
        
    try:
        try:
            converted_date = pd.to_datetime(date_series, format=input_format_, errors='coerce').dt.strftime(output_format_)
        except:
            converted_date = pd.to_datetime(date_series, format=input_format_,errors='coerce',utc=True).dt.strftime(output_format_)

        logging.info(f"Got converted date is : {converted_date}")
        result['flag'] = True
        result['data'] = {"value": converted_date[0]}
        
    except Exception as e:
        logging.info("Failed while Converting date into given format")
        logging.info(e)
        result['flag'] = False
        result['data'] = {'reason':'cannnot convert given date to required format','error_msg':str(e)}
    
    return result

def get_data(tenant_id, database, table, case_id, case_id_based=True , view='records'):
    """give the data from database
    Args:
        
    Returns:
        result (dict)
        result is a dict having keys flag, data.Depending on the flag the data changes.
        if flag is True: data contains the key value and value is the data.
        if flag is False: data contains the error_msg say why its failed.
    Example:
            x = get_data('invesco.acelive.ai','extraction','ocr','INV4D15EFC')"""
    result = {}
    db = DB(database, tenant_id = tenant_id, **db_config)
    logging.info(f"case_id based: {case_id_based}")
    try:
        if case_id_based:
            query = f"SELECT * from `{table}` WHERE `case_id` = '{case_id}'"
            try:
                df = db.execute(query)
            except:
                df = db.execute_(query)
            table_data = df.to_dict(orient= view)
            result['flag'] = True
            result['data'] = {"value":table_data}
            
        else:
            query = f"SELECT * from `{table}`"
            df = db.execute(query)
            if not df.empty:
                table_data = df.to_dict(orient = view)
            else:
                table_data = {}
            result['flag'] = True
            result['data'] = {"value":table_data}
    except Exception as e:
        logging.error(f"Failed in getting tables data from database")
        logging.error(e)
        result['flag'] = 'False'
        result['data'] = {'reason':'Failed in getting tables data from database','error_msg':str(e)}
    return result

def save_data(tenant_id, database, table, data, case_id, case_id_based=True, view='records'):
    """Util for saving the data into database
    
    Args:
        tenant_id (str) -> the tenant name for which we have to take the database from. ex.invesco.acelive.ai
        database (str) -> database name. ex.extraction
        table (str) -> table name. ex.ocr
        case_id_based (bool) -> says whether we have to bring in all the data or only the data for a case_id.
        case_id (str) -> case_id for which we have to bring the data from the table.
        data (dict) -> column_value map or a record in the database.
    Returns:
        result (dict)
    Example:
        data1 = {'ocr':{'comments':'testing','assessable_value':1000}}
        save_data(tenant_id='deloitte.acelive.ai', database='extraction', table='None', data=data1, case_id='DEL754C18D_test', case_id_based = True, view='records')"""
    logging.info(f"tenant_id got is : {tenant_id}")
    logging.info(f"database got is : {database}")
    logging.info(f"table name got is : {table}")
    logging.info(f"data got is : {data}")
    logging.info(f"case_id got is : {case_id}")
    result = {}
    
    if case_id_based:
        logging.info(f"data to save is case_id based data.")
        try:

            db_config['tenant_id'] = tenant_id
            if database == 'extraction':
                extraction_db = DB('extraction', **db_config) # only in ocr or process_queue we are updating
            elif database == 'queues':
                queue_db = DB('queues', **db_config) # only in ocr or process_queue we are updating

            for table, colum_values in data.items():
                if table == 'ocr':
                    extraction_db.update(table, update=colum_values, where={'case_id':case_id})
                if table == 'process_queue':
                    queue_db.update(table, update=colum_values, where={'case_id':case_id})
        except Exception as e:
            logging.error(f"Cannot update the database")
            logging.error(e)
            result["flag"]=False,
            result['data'] = {'reason':f'Cannot update the database','error_msg':str(e)}
            return result
        result['flag']=True
        result['data']= data
        return result

    else:
        logging.info(f"data to save is master based data.")
        try:
            db_config['tenant_id'] = tenant_id
            extraction_db = DB('extraction', **db_config) # only in ocr or process_queue we are updating
            queue_db = DB('queues', **db_config) # only in ocr or process_queue we are updating

            for table, colum_values in data.items():
                logging.info('************** have to develop due to where clause condition not getting from data *******')
        except Exception as e:
            logging.error(f"Cannot update the database")
            logging.error(e)
            result['flag']=False
            result['data'] = {'reason':f'Cannot update the database','error_msg':str(e)}
        
        result['flag']=True
        result['data']= data  
        return result
