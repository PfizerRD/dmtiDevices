import h5py
import numpy as np
import pandas as pd
import os
from tqdm import tqdm
import seaborn as sns
import matplotlib.pyplot as plt
import logging
import datetime
#import botocore
import sys
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/') #path where the file is downloaded
from helpers import *

###Logger config
logging.basicConfig(filename='Balance_task_errors.log',level=logging.ERROR)
logging.log(level=logging.ERROR, msg = 'Current date: %s' % str(datetime.datetime.now()))

def get_start_time(filename):
    '''
    This function takes the task file and extracts the start time from the initial contacts
    :param filename:
    :return: the start and end time of the fil, in the appropriate unit (sec)
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
    Convert a hdf object to data frame and take median
    :param hdf_data: This is hdf hdf object
    :return: the median of the data
    '''
    npdf = np.array(hdf_data)
    df = pd.DataFrame(npdf)

    median_data = np.nanmedian(df)

    return median_data

def QC_raw_APDM(row, task):

    ### check the task file
    d = h5py.File(row.APDM_task_filename, 'r')
    sensors = list(d['Sensors'].keys())
    if len(sensors) != 6:
        num_sensors = 'No'
    else:
        num_sensors = 'Yes'

    # Check Accel data completeness
    accel_raw_data_len = []
    gyro_raw_data_len = []
    for sen in sensors:
        try:
            sensor_mean_time = (1 / (np.mean(np.diff(np.array(d['Sensors'][sen]['Time'])))))
            sensor_shape = d['Sensors'][sen]['Accelerometer'].shape
            shape_r = {'sensor': sen, 'rows': sensor_shape[0], 'cols': sensor_shape[1],
                       'samp_rate': sensor_mean_time}
            accel_raw_data_len.append(shape_r)
            #repeat for gyro
            sensor_shape = d['Sensors'][sen]['Gyroscope'].shape
            shape_r = {'sensor': sen, 'rows': sensor_shape[0], 'cols': sensor_shape[1]}
            gyro_raw_data_len.append(shape_r)
        except IndexError:
            print('accel data not available')
    accel_raw_data_len = pd.DataFrame(accel_raw_data_len)
    gyro_raw_data_len = pd.DataFrame(gyro_raw_data_len)

    ### check for the same length in all files ()
    short_accel_files = []
    time_check_files = []
    for i, r in accel_raw_data_len.iterrows():
        if r.cols != accel_raw_data_len.cols.mean():
            print('short columns')
            if r.rows != accel_raw_data_len.rows.mean():
                short_accel_files.append(r.sensor)
                if r.samp_rate != accel_raw_data_len.samp_rate.mean():
                    time_check_files.append(r.sensor)
    short_gyro_files = []
    for i, r in gyro_raw_data_len.iterrows():
        if r.cols != gyro_raw_data_len.cols.mean():
            print('short columns')
            if r.rows != gyro_raw_data_len.rows.mean():
                short_gyro_files.append(r.sensor)

    if len(short_accel_files) == 0:
        acc_len = 'x'
    else:
        acc_len = 'No'
    if len(short_gyro_files) == 0:
        gyro_len = 'x'
    else:
        gyro_len = 'No'

    if len(time_check_files) == 0:
        time_len = 'x'
    else:
        time_len = 'No'

    return num_sensors, acc_len, gyro_len, time_len

def get_apdm_balance(row, task):
    '''

    :param subject: 12 digit subject #
    :param visit: the visit #
    :param filename: the analysis filename
    :return: a row of data frame with the subjects posture data
    '''

    #Specify the subject, APDM analysis file, and the visit
    subject = row['subject']
    filename = row['APDM_analysis_filename']
    visit = row['visit']

    # load the .h5 file
    #data = h5py.File(filename, 'r')
    #TODO: need to fix the errors

    data = read_file_aws(row.bucket, row.APDM_analysis_filename, '.h5')
    #except client.exceptions.NoSuchKey

    ### See if the analysis file has the proper fields
    d_types = []
    try:
        d_types = list(data['Measures']['Postural Sway'].keys())
    except KeyError:
        ### if not (this needs to be logged -- but shouldnt be a problem, becuase we filter out if file exists
        logging.log(level = logging.ERROR, msg = 'sub %s, visit %s does not have postural sway measures' %
                                                 (subject, visit))
        print('{} has no keys'.format(subject))
        pass

    metrics = [] #names for balance values
    value = []  #value for metric

    for name in d_types:
        fields = list(data['Measures']['Postural Sway'][name].keys())
        for data_name in fields:
            value.append(hdf_to_data(data['Measures']['Postural Sway'][name][data_name]))
            metrics.append(name + ' ' + data_name)

    ## check the fields for Nan values
    num_nan = sum(np.isnan(value))
    if num_nan > 0:
        na_issue = 'Missing data in fields'
    else:
        na_issue = 'No'

    ### check the task file:
    try:
        num_sensors, acc_len, gyro_len, time_len = QC_raw_APDM(row, task)
        qc_r = {'subject': row.subject,
                'visit': row.visit,
                'task': task,
                'analysis_file_exists': 'x',
                'NA_in_metrics': na_issue,
                'raw_file_exists': 'x',
                '6_sensors_wData': num_sensors,
                'accel_same_length': acc_len,
                'gyro_same_length': gyro_len,
                'sample_time_same': time_len}
    except OSError:
        #print('%s %s %s missing task .h5 file' % (subject, visit, task))
        qc_r = {'subject': row.subject,
                'visit': row.visit,
                'task': task,
                'analysis_file_exists': 'x',
                'NA_in_metrics': na_issue,
                'raw_file_exists': 'No',
                '6_sensors_wData': '',
                'accel_same_length': '',
                'gyro_same_length': '',
                'sample_time_same': ''}


    return value, metrics, visit, task, subject, qc_r

def get_balance_files(task):
    '''

    :param task: This is either "side", "semi", or "tandem"
    :return: data frame with the subject and the two APDM files for the subject

    '''
    path = '/Volumes/npru-bluesky/OtherProjects/STEPP/code/s3_data/raw/'
    subj_list = get_subject_list()

    df = []
    visits = 'Visit_1', 'Visit_2'
    for i in range(len(subj_list)):
        subject = subj_list[i]
        for j in range(len(visits)):
            toss2, visit_num = visits[j].split('_')

            if task == 'sway':

                APDM_task_filename = path + subject + '/' \
                              + visits[j] + '/APDM/' + subject + '_0' + visit_num + \
                              '_OPAL_Sway.h5'
                APDM_analysis_filename = path + subject + '/' \
                              + visits[j] + '/APDM/' + subject + '_0' + visit_num +  \
                              '_OPAL_Sway_Analysis.h5'
            if task == 'semi_tandem':
                APDM_task_filename = path + subject + '/' \
                              + visits[j] + '/APDM/' + subject + '_0' + visit_num +  \
                              '_OPAL_Sway_semi_tandem.h5'
                APDM_analysis_filename = path + subject + '/' \
                              + visits[j] + '/APDM/' + subject + '_0' + visit_num +  \
                              '_OPAL_Sway_semi_tandem_Analysis.h5'
            if task == 'tandem':
                APDM_task_filenme = path + subject + '/' \
                              + visits[j] + '/APDM/' + subject + '_0' + visit_num +  \
                              '_OPAL_Sway_tandem.h5'
                APDM_analysis_filename = path + subject + '/' \
                              + visits[j] + '/APDM/' + subject + '_0' + visit_num +  \
                              '_OPAL_Sway_tandem_Analysis.h5'

            row = {'subject': subject,
                   'visit': visit_num,
                   'APDM_task_filename': APDM_task_filename,
                   'APDM_analysis_filename': APDM_analysis_filename}

            df.append(row)

    df = pd.DataFrame(df)
    return df

def run_STEPP_balance_APDM():
    '''
    This script will extract metrics from the APDM system balance tasks

    :return: Return 3 csv for the 3 balance tasks with the metrics from APDM system broken down by subject and visit
    '''
    sway_data = []
    df = get_STEPP_files_AWS('Sway', '.h5')
    for index, row in tqdm(df.iterrows()):
        try:
            rowp, metrics, visit, task, subject, qc_r = get_apdm_balance(row, row.task)
        except UnboundLocalError:
            print(row)
            continue

        r = pd.DataFrame(rowp).T
        r.columns = metrics
        r['subject'] = subject
        r['visit'] = visit
        r['task'] = task

        sway_data.append(r)

    sway_df = pd.concat(sway_data)
    sway_df.to_csv(
        '/Users/psaltd/Desktop/UNC_cachexia/Processed_data/APDM/STEPP_APDM_Balance_Sway_features.csv',
        index=False)

if __name__ == '__main__':
    run_STEPP_balance_APDM()

