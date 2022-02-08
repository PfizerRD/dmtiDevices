#Import libraries
import sys
sys.path.insert(1, '/Users/bluesky/Documents/GitHub/X9001262_QC/src/utility/')
from get_GAITRite_files import get_PKMAS_files_AWS
from src.utility.get_GAITRite_files import *

sys.path.insert(1, '/Users/bluesky/Documents/GitHub/STEPP/src/')
from helpers import read_file_aws

import numpy as np

#Processing of PKMAS data
def get_PKMAS_medians(row):
    #Read file from AWS and take median of metrics
    df = read_file_aws(row.S3_obj.bucket_name, row.S3_obj.key, '.csv')
    columns = df.iloc[10, :]
    sub_df = df.iloc[26:, :]
    sub_df.columns = columns
    #Need to reduce dataframe to isolate the raw data
    medians_subdf = np.nanmedian(sub_df.iloc[:, 6:].astype('float'), axis=0)
    medians_subdf = pd.DataFrame([medians_subdf])
    medians_subdf.columns = sub_df.iloc[:, 6:].columns

    if columns[5:].isnull().values.any(): #Most likely a formatting error on the file from PKMAS (skip 1 more row)
        columns = df.iloc[11, :]
        sub_df = df.iloc[27:, :]
        sub_df.columns = columns
        medians_subdf = np.nanmedian(sub_df.iloc[:, 6:].astype('float'), axis=0)
        medians_subdf = pd.DataFrame([medians_subdf])
        medians_subdf.columns = sub_df.iloc[:, 6:].columns
    else:
        pass
    #Get the Cadence (mean - steps/min)
    cadence_col = df.iloc[:,3].dropna().reset_index(drop = True)
    if 'Cadence' in cadence_col.iloc[0]: #inspect the column to make sure its correct!
        cadence = float(cadence_col.iloc[1])
    else:
        raise ValueError

    #Join file metadata for indexing
    meta_obj = pd.DataFrame([{'subject': row.filename.split('_')[1],
                              'visit': row.visit,
                              'task': row.task,
                              'mean_cadence': cadence}])

    medians_subdf = meta_obj.join(medians_subdf)

    return medians_subdf

if __name__ == '__main__':
    csv = '/Users/psaltd/Downloads/X9001262_A_10010019_01_Slow_PKMAS.csv'
    #get_PKMAS_medians(csv)

    results = []
    df = get_PKMAS_files_AWS('pkmas')
    no_partials = df[df.subject == 'X9001262_CohortA_noPartials']
    no_partials.reset_index(drop=True)#[x for x in df.subject if '_noPartials' in x]
    for index, row in no_partials.iterrows():
        out = get_PKMAS_medians(row)
        if np.nan in out:
            raise Warning
        results.append(out)

    res_df = pd.concat(results)
    res_df.to_csv('/Users/bluesky/Documents/X9001262/Data_Science/X9001262_inClinic_Gait_PKMAS_Medians.csv',
                      index=False)
