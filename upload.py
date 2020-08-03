# Author: Augusto Espin
# Test of RQ for task

import time
from rq import get_current_job
from rq.decorators import job

import numpy as np
import pandas as pd 

from preprocess import preprocess
from process import process
from geo_amc import GeoOperations
from emissions import parameters

emission_data = [{'name': 'ghg30',
                  'parameters': parameters,
                  'ratio': 0.3,
                  'bus':   'no'},
                  {'name': 'ghg50',
                  'parameters': parameters,
                  'ratio':0.5,
                  'bus':  'no'},
                  {'name': 'bus',
                  'parameters': parameters,
                  'ratio':0.3,
                  'bus': 'bus'},
                  {'name': 'grp',
                  'parameters': parameters,
                  'ratio':0.3,
                  'bus': 'group'},
                 ]

def send_message(job, message, progress):
    job.refresh
    msg = job.meta['task'] + '\n' + message
    job.meta['progress'] = progress
    job.meta['task'] = msg
    job.save_meta()

@job("amc-tasks", timeout = 1500)
def upload_and_process(dbstring, apikey, year, data):
    '''
    Worker thread for the pipeline
    '''

    try:
        # Initialize message structure with current job
        job = get_current_job()
        message = { 'send': send_message,
                    'job': job}

        # Initialize the dataframe with the data passed by user
        df = pd.DataFrame(data)
        
        # Initialize function that sends data
        snd = message['send']
        jb = message['job']

        # Initialize the message queue
        print('Starting preprocessing...')
        job.meta['progress'] = 0
        job.meta['task'] = 'Starting Preprocessing...'
        job.meta['year'] = year
        job.save_meta()

        # Intialize geo operations object
        geo = GeoOperations(uszipfile='us-zip-code-latitude-and-longitude.csv',key=apikey,dbstring = dbstring)
        pp = preprocess(geo)
        pr = process(geo,dbstring)
        
        # Initialize pre-processor
        ts = time.perf_counter()
        df1, df_err, df_invalid = pp.execute(df,message=message)
        te = time.perf_counter()
        dt1 = (te-ts)/60

        # Preprocessing finished
        snd(jb,f"Preprocessing finished in {dt1} min.", 20)
        
        time.sleep(1)
        # Processing starts
        snd(jb,"Starting Processing...",22)

        # Start processing
        ts = time.perf_counter()
        tbls = pr.execute(df1, year)
        te = time.perf_counter()
        dt2 = (te-ts)/60

        # Processing finished
        snd(jb,f"Processing Finished in {dt2} min.", 91)

        # Compute emissions
        snd(jb,f"Computing emissions...", 92)
        # Get itininerary table
        itinerary = pd.read_sql_table('itinerary',dbstring)
        ghg = pr.compute_emissions(itinerary,emission_data)

        snd(jb,f"Emissions computed.", 97)
        # Updating errors
        df_prob = pd.DataFrame({'year':[year],
                                'invalid':[df_invalid.shape[0]],
                                'errors': [df_err.shape[0]],
                                'total': [df.shape[0]]})
        
        # Try to update the error database
        try:
            df_prob1 = pd.read_sql_table('processing_err',dbstring)
            df_prob1 = df_prob1.append(df_prob, ignore_index=True)
            df_prob1.to_sql('processing_err',dbstring, index=False, if_exists='replace')
        except:
            # Table does not exists, create the table
            df_prob.to_sql('processing_err', dbstring, index=False, if_exists='replace')

        time.sleep(1)
        # Pipeline finished
        snd(jb,f"Pipeline Finished in {dt1 + dt2} min.", 100)

    except:
        print("Job execution failed")
        snd(jb,"Failed.", 100)
    