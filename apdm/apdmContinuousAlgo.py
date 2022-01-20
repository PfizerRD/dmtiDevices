import h5py
import pandas as pd
import numpy as np
from src.utility.get_APDM_files import get_APDM_ContAlgo_files_AWS

import sys
sys.path.insert(1, '/Users/bluesky/Documents/GitHub/STEPP/src/') #path where the file is downloaded
from helpers import *

def get_ContAlgo_strides(data):
    steps = []
    steps = data['Bouts']['gaitIndices'].value
    l, w = steps.shape
    max_strides = steps[l-1][w-1]
    total_strides = (max_strides + 1)*2

    return total_strides


def process_APDM_Cont_Algo(data):
    #Bouts: - Use these bout times (from James McNames)
    #data['Bouts'].keys()

    #Metrics - Gait
    gait_data = data['Metrics']['Gait']['Feet']
    results = []
    metrics = []
    for key in gait_data.keys():
        validity = pd.DataFrame(gait_data[key]['validity'].value)
        values = pd.DataFrame(gait_data[key]['values'].value)

        validity.columns = ['valL', 'valR']
        values.columns = ['valuesL', 'valuesR']
        #joint_df = pd.concat([validity, values], axis = 1)
        #results.append({'metric': key,
        #                'median_value': np.nanmedian(values)})

        metrics.append(key)
        results.append(np.nanmedian(values))
    turns_data = data['Metrics']['Turns']['Lumbar']
    for key in turns_data.keys():
        try:
            validity = pd.DataFrame(turns_data[key]['validity'].value)
            values = pd.DataFrame(turns_data[key]['values'].value)

            #results.append({'metric': 'Turns-' + key,
            #                'median_value': np.nanmedian(values)})
            metrics.append(key)
            results.append(np.nanmedian(values))
        except ValueError:
            datetimes = data['Metrics']['Turns']['Lumbar'][key].value

    obj = pd.DataFrame([results])
    obj.columns = metrics

    obj['total_strides'] = get_ContAlgo_strides(data)

    return obj

if __name__ == '__main__':
    get_APDM_ContAlgo_files_AWS()
    #file = '/Users/bluesky/Downloads/X9001262_A_10010001_01_OPAL_20minWalk_Continuous_Analysis.h5'
    #data = h5py.File(file, 'r')
    APDM_files = get_APDM_ContAlgo_files_AWS()
    results = []
    for index, row in APDM_files.iterrows():
        data = read_file_aws(row.S3_obj.bucket_name, row.S3_obj.key, '.h5')
        try:
            results_df = process_APDM_Cont_Algo(data)
        except KeyError:
            continue
        new_obj = {'subject': row.subject,
                   'visit': row.visit,
                   'task': row.task}
        res_obj = pd.concat([pd.DataFrame([new_obj]), results_df], axis=1)
        results.append(res_obj)

    res_df = pd.concat(results)
    res_df.to_csv('/Users/bluesky/Documents/X9001262/Data_Science/X9001262_APDM_ContAlgo_Medians.csv', index = False)
