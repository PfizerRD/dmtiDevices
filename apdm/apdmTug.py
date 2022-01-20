import os
import seaborn as sns
import matplotlib.pyplot as plt
import logging
import datetime
import h5py
import numpy as np
import pandas as pd
from tqdm import tqdm

import sys
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/') #path where the file is downloaded
from helpers import *

#Logger Config
logging.basicConfig(filename='Tug_task_errors.log',level=logging.DEBUG)
logging.warning('Issues with Tug task data')
logging.warning('Current date: %s' % datetime.datetime.now())

def get_start_time(filename):
    '''
    This function takes the task file and extracts the start time from the initial contacts
    :param filename:
    :return: return the start time in unix
    '''
    task_data = h5py.File(filename, 'r')
    task_st = task_data['Sensors']['670']['Time']
    initial_start = np.array(task_st)
    initial_start = pd.DataFrame(initial_start)
    initial_start.columns = ['time']
    initial_start = initial_start / 1000000
    start_time = initial_start.time[0]
    end_time = initial_start.time[-1:]

    return start_time, end_time

def hdf_to_data(hdf_data):
    '''

    :param hdf_data: column from H5 file
    :param metric: the name of the data array metric
    :return: the median value and the respective name of the metric
    '''

    npdf = np.array(hdf_data)
    df = pd.DataFrame(npdf)

    median_data = np.nanmedian(df)

    return median_data


def get_apdm_tug_features(row):

    '''

    :param subject: 12 digit subject #
    :param visit: the visit #
    :param filename: the analysis filename
    :return:
    '''

    tug_data = []
    Cnames = []

    subject = row.subject
    visit = row.visit
    #try:
    #    data = h5py.File(filename, 'r')
    #except KeyError:
    #    logging.log(level=logging.WARNING, msg=subject + '_' + visit + '_tug_' + 'does not exist')
    data = read_file_aws(row.bucket, row.APDM_analysis_filename, '.h5')
    measure_type = list(data['Measures'].keys())
    for m_type in measure_type:
        feature_data = data['Measures'][m_type]
        if len(feature_data) == 1:
            duration = np.array(feature_data).item()
        else:
            metrics_list = list(data['Measures'][m_type])
            for metric in metrics_list:
                coln = '%s - %s' % (m_type, metric)
                tug_data.append(hdf_to_data(data['Measures'][m_type][metric]))
                Cnames.append(coln)

    tug_df = pd.DataFrame(tug_data).T

    tug_df.columns = Cnames

    tug_df['duration'] = duration
    tug_df['subject'] = subject
    tug_df['visit'] = visit

    return tug_df

def run_STEPP_TUG_APDM():
    df = get_STEPP_files_AWS('TUG', '.h5')

    APDM_metrics = []
    for index, row in tqdm(df.iterrows()):
        row = get_apdm_tug_features(row)
        APDM_metrics.append(row)

    APDM_data = pd.concat(APDM_metrics, axis=0)
    APDM_data = APDM_data.sort_values(by='subject')

    APDM_data.to_csv('/Users/psaltd/Desktop/UNC_cachexia/Processed_data/APDM/'
                     'STEPP_APDM_Tug_features.csv', index = False)

if __name__ == '__main__':
    run_STEPP_TUG_APDM()
