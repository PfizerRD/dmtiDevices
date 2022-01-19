import h5py
import numpy as np
import pandas as pd
from tqdm import tqdm
import logging
import os
import datetime

import sys
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/') #path where the file is downloaded
#sys.path.insert(1, '/Users/bluesky/Documents/GitHub/STEPP/src/') #path where the file is downloaded
from helpers import *

#Logger Config
logging.basicConfig(filename='Gait_task_errors.log' ,level=logging.ERROR)
logging.log(level =logging.ERROR, msg ='Current date: %s' % str(datetime.datetime.now()))

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


def get_apdm_gait_features(row):

    '''

    :param subject: 12 digit subject #
    :param visit: the visit #
    :param filename: the analysis filename
    :return:
    '''
    #try:
    #    #data = h5py.File(filename, 'r')
    #except KeyError:
    #    logging.log(level=logging.ERROR, msg='%s visit %s cannot open APDM Gait file' % (subject, visit))

    subject = row.subject
    visit = row.visit
    data = read_file_aws(row.bucket, row.APDM_analysis_filename, '.h5')
    try:
        data_steps_left = data['Events']['Gait']['Lower Limb']['All Steps Left']
        data_steps_right = data['Events']['Gait']['Lower Limb']['All Steps Right']
        data_steps = len(data_steps_left) + len(data_steps_right)
        lower_limb_data_path = data['Measures']['Gait']['Lower Limb']

        d_types = list(lower_limb_data_path.keys())
        metrics = []
        value = []

        for name in d_types:
            value.append(hdf_to_data(lower_limb_data_path[name]))
            metrics.append(name)


        gait_df = pd.DataFrame(value).T
        gait_df.columns = metrics

        if row.task:
            meta_obj = pd.DataFrame([{'subject': subject, 'visit': visit, 'task': row.task, 'steps': data_steps}],
                                    columns=['subject', 'visit', 'task', 'steps'])
        else:
            meta_obj = pd.DataFrame([{'subject': subject, 'visit': visit, 'steps': data_steps}],
                                    columns=['subject', 'visit', 'steps'])
        #gait_df['subject'] = subject
        #gait_df['visit'] = visit
        #gait_df['steps'] = data_steps
        gait_df = meta_obj.join(gait_df)
    except ValueError:
        logging.log(level=logging.ERROR, msg='%s visit %s does not have APDM Gait Data' % (subject, visit))
        gait_df = []
    except KeyError:
        logging.log(level=logging.ERROR, msg='%s visit %s is missing APDM Gait Data' % (subject, visit))
        gait_df = []

    return gait_df

def get_walk_files():
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
                          '_OPAL_Walk.h5'

            APDM_analysis_csv = path + subject + '/' \
                          + visits[j] + '/APDM/' + subject + '_0' + visit_num + \
                          '_OPAL_Walk_Analysis.h5'

            row = {'subject': subject,
                   'visit': visit_num,
                   'APDM_task_filename': APDM_task_csv,
                   'APDM_analysis_filename': APDM_analysis_csv}
            df.append(row)
    df = pd.DataFrame(df)

    return df

def run_STEPP_APDM_gait():
    df = get_STEPP_files_AWS('Walk', '.h5')

    APDM_metrics = []
    for index, row in tqdm(df.iterrows()):
        dat_row = get_apdm_gait_features(row)
        if len(dat_row) == 0:
            continue
        APDM_metrics.append(dat_row)

    APDM_data = pd.concat(APDM_metrics, axis=0)
    APDM_data = APDM_data.sort_values(by='subject')

    APDM_data.to_csv('/Users/psaltd/Desktop/UNC_cachexia/Processed_data/APDM/' \
        'STEPP_APDM_gait_features.csv', index=False)

if __name__ == '__main__':
    run_STEPP_APDM_gait()
