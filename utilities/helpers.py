import os
import pandas as pd
from tqdm import tqdm
from pfawsaccess import *
import io
import boto3
from pathlib import Path
import h5py
import botocore

def download_aws():
    from os import makedirs, sep
    from tqdm import tqdm
    bucket = 'ecddmtisteppamrasp67973'
    include = ['.bin', '.h5', '.csv']  # include files with .bin and .h5 in their names
    # exclude files with the following in their names. In this case look for full trial data
    # from APDM (.h5, in-lab) and GENEActive (.bin, at-home)
    #exclude = ['wrist', 'cortrium', 'dynamometer', 'Sit_to_stand', 'walk', 'wood', 'lumbar']
    exclude =  ['Walk', 'Sway', 'Sit_to_Stand', 'TUG', 'DIGDYNAMO', 'APDM', 'Cortrium',
     'red_cap', 'archive', 'REDCap', '.bin']
    #exclude = ['.txt']

    obj_paths = get_object_paths(bucket, prefix='raw', include=include, exclude=exclude, case_sensitive=False)

    for path in tqdm(obj_paths):
        local_path = f'/Volumes/npru-bluesky/OtherProjects/STEPP/code/s3_data/{path}'
        #local_path = f'/Users/psaltd/Desktop/UNC_cachexia/{path}'
        #local_path = f'/Users/psaltd/Desktop/{path}'
        # only check the directory part
        makedirs(sep.join(local_path.split('/')[:-1]), exist_ok=True)
        # download from the bucket, with the bucket path, to the local path
        download_object(bucket, path, local_path)

def filter_files(files, include, exclude, extension):
    if '*' in exclude:
        mask = [False] * len(files)

        for i, file in enumerate(files):
            mask[i] |= (file.suffix.lower() == extension.lower())
            mask[i] |= any([s.lower() in str(file).lower() for s in include]) or (include == [])
    elif '*' in include:
        mask = [True] * len(files)

        for i, file in enumerate(files):
            mask[i] &= (file.suffix.lower() == extension.lower())
            mask[i] &= not any([s.lower() in str(file).lower() for s in exclude])

    else:
        mask = [True] * len(files)

        for i, file in enumerate(files):
            mask[i] &= (file.suffix.lower() == extension.lower())
            mask[i] &= any([s.lower() in str(file).lower() for s in include]) or (include == [])
            mask[i] &= not any([s.lower() in str(file).lower() for s in exclude])

        for i in range(len(mask) - 1, -1, -1):
            if not mask[i]:
                del files[i]

    return files

def read_file_aws(bucket, path, ext):
    import boto3
    import pandas as pd
    import io

    session = boto3.Session(profile_name='saml')
    s3 = session.resource('s3')

    obj = s3.Object(bucket, str(path))

    if ext == '.csv':
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
    if ext == '.h5':
        df = h5py.File(io.BytesIO(obj.get()['Body'].read()))

    return df