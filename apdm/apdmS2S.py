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
logging.basicConfig(filename='S2S_task_errors.log',level=logging.ERROR)
logging.log(level = logging.ERROR, msg ='Current date: %s' % datetime.datetime.now())

def get_s2s_files():
    path = '/Volumes/npru-bluesky/OtherProjects/STEPP/code/s3_data/raw/'
    subj_list = get_subject_list()

    df = []
    visits = 'Visit_1', 'Visit_2'
    for i in range(len(subj_list)):
        subject = subj_list[i]
        for j in range(len(visits)):
            toss2, visit_num = visits[j].split('_')

            APDM_task_csv = path + subject + '/' \
                          + visits[j] + '/APDM/' + subject + '_0' + visit_num + \
                          '_OPAL_Sit_to_Stand.h5'
            APDM_analysis_csv = path + subject + '/' \
                          + visits[j] + '/APDM/' + subject + '_0' + visit_num + \
                          '_OPAL_Sit_to_Stand_Analysis.h5'

            row = {'subject': subject,
                   'visit': visit_num,
                   'APDM_task_filename': APDM_task_csv,
                   'APDM_analysis_filename': APDM_analysis_csv}
            df.append(row)

    df = pd.DataFrame(df)

    return df

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

def get_apdm_s2s_features(row):

    '''

    :param subject: 12 digit subject #
    :param visit: the visit #
    :param filename: the analysis filename
    :return:
    '''

    s2s_data = []
    Cnames = []
    subject = row.subject
    visit = row.visit

    #try:
    #    data = h5py.File(filename, 'r')
    #except KeyError:
    #    logging.log(level=logging.WARNING, msg=subject + '_' + visit + '_S2S_' + 'does not exist')
    #    print(filename + '_' + visit + 'S2S Analysis file has error ')
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
                s2s_data.append(hdf_to_data(data['Measures'][m_type][metric]))
                Cnames.append(coln)

    s2s_df = pd.DataFrame(s2s_data).T

    s2s_df.columns = Cnames

    s2s_df['Duration'] = duration
    s2s_df['subject'] = subject
    s2s_df['visit'] = visit

    return s2s_df

def run_STEPP_s2s_APDM():
    df = get_STEPP_files_AWS('Sit_to_Stand', '.h5')
    APDM_metrics = []
    for index, row in tqdm(df.iterrows()):
        rowp = get_apdm_s2s_features(row)
        APDM_metrics.append(rowp)

    APDM_data = pd.concat(APDM_metrics, axis=0)
    APDM_data = APDM_data.sort_values(by='subject')

    APDM_data.to_csv('/Users/psaltd/Desktop/UNC_cachexia/Processed_data/APDM/STEPP_APDM_s2s_features.csv', index = False)


if __name__ == '__main__':
    run_STEPP_s2s_APDM()