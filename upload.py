# Author: Augusto Espin
# Test of RQ for task

import time
from rq import get_current_job

def upload_and_process(seconds, year, data):
    job = get_current_job()
    print('Starting task')
    for i in range(seconds):
        print(f'{i}: Doing task for year: {year}, processing file: {data}')
        job.meta['progress'] = int(100 * i/seconds)
        job.meta['task'] = 'Preprocessing'
        job.meta['year'] = year
        job.save_meta()
        time.sleep(1)
    
    job.meta['progress'] = 100
    job.save_meta()

    print('Task completed')