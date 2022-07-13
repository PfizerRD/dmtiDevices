# HRV metrics
# Funtions to compute metrics described in https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5624990/#:~:text=It%20is%20calculated%20by%20first,average%20of%20these%20288%20values.

import numpy as np
import pandas as pd


# TIME-DOMAIN METRICS
def compute_sdnn(data, intype='intervals'):
    """
    DESCRIPTION:
    Standard Deviation of N-N intervals: standard deviation of normal (i.e. input signal has undegone QC to remove erroneous R-wave detection) R-R intervals
    ----------
    INPUTS (* REQUIRED):
    >> *data: LIST, ARRAY, or ARRAYLIKE
    >> intype: STR
        L>> keyword specifying input data format
        L>> default value of 'intervals' will expect data to be list of N-N intervals
        L>> 'timestamps' will expect data to be a list of timestamps corresponding to normal R-wave peaks (timestamps must be sequential)
    ----------
    OUTPUTS:
    >> sdnn: shares TYPE with elements in data
    """
    if intype == 'intervals':
        sdnn = np.std(data)
    elif intype == 'timestamps':
        sdnn = np.std(pd.Series(data).diff().dropna().to_list())
    return sdnn


# SDANN was mentioned in the source paper, but was noted not to produce much additional useful information beyond what is found by computing SDNN.
# If it is needed this function can be finished, but it doesn't seem to be an efficient use of time

# def compute_sdann(data, intype = 'intervals', epochsize = 300000, unit = 'ms'):
#     """
#     DESCRIPTION:
#     Standard Deviation of Average N-N intervals: SDNN of the average N-N intervals when signal is split into epochs (default 5min epochs)
#     ----------
#     INPUTS (* REQUIRED):
#     >> *data: LIST, ARRAY, or ARRAYLIKE
#     >> intype: STR
#         L>> keyword specifying input data format
#         L>> default value of 'intervals' will expect data to be list of N-N intervals
#         L>> 'timestamps' will expect data to be a list of timestamps corresponding to normal R-wave peaks (timestamps must be sequential)
#     >> epochsize: INT or FLOAT
#         L>> defines size of epochs for averaging (NOTE: epochsize will use units specified by unit optional input)
#         L>> default value is 300,000 (# of milliseconds in 5 minutes)
#     >> unit: STR
#         L>> keyword specifying time units used in data
#         L>> uses 'ms' (milliseconds) by default, other valid inputs:
#             L>> 's' (seconds),'m' (minutes)
#     ----------
#     OUTPUTS:
#     >> sdann: FLOAT
#     """
#     if intype == 'intervals':
#         pass
#     elif intype == 'timestamps':
#         averages = [None for x in range(np.ceil((max(data)-min(data))/epochsize))] # NOTE: will compute epochs from
#         k = 0

def compute_sdnni():  # TODO
    """
    DESCRIPTION:
    ----------
    INPUTS (* REQUIRED):
    ----------
    OUTPUTS:
    """
    pass


def compute_pnn50(data, intype='intervals', outtype='percentage'):
    """
    DESCRIPTION:
    Percentage of successive N-N intervals that are different by more than 50ms (can also return NN50)
    ----------
    INPUTS (* REQUIRED):
    >> *data: LIST, ARRAY, or ARRAYLIKE
        L>> NOTE: must be in units of milliseconds
    >> intype: STR
        L>> keyword specifying input data format
        L>> default value of 'intervals' will expect data to be list of N-N intervals
        L>> 'timestamps' will expect data to be a list of timestamps corresponding to normal R-wave peaks (timestamps must be sequential)
    >> outtype: STR
        L>> keyword specifying output data units
        L>> default value of 'percentage' will give pNN50
        L>> 'count' will give NN50
    ----------
    OUTPUTS:
    >> pnn50: FLOAT
    """

    if intype == 'intervals':
        diffs = pd.Series(data).diff().dropna().apply(
            lambda x: abs(x))  # absolute change between successive interval lengths
        nn50 = len(diffs.loc[diffs > 50])
    if intype == 'timestamps':
        diffs = pd.Series(data).diff().diff().dropna().apply(lambda x: abs(
            x))  # first diff() gets NN intervals, seconds diff() gets change between successive interval lengths
        nn50 = len(diffs.loc[diffs > 50])
    pnn50 = 100 * (nn50 / len(diffs))

    if outtype == 'percentage':
        return pnn50
    elif outtype == 'count':
        return nn50


def compute_hr_range(data, intype='bpm'):
    """
    DESCRIPTION:
    HRmax - HRmin
    NOTE: will compute this for whole segment, does not subdivide into epochs
    ----------
    INPUTS (* REQUIRED):
    >> *data: LIST, ARRAY, or ARRAYLIKE
    >> intype: STR
        L>> keyword specifying input data format
        L>> default value of 'bpm' will expect data to be list of heart rate values
        L>> 'ms' will expect data to be list of N-N intervals in milliseconds
        L>> 's' will expect data to be list of N-N intervals in seconds
    ----------
    OUTPUTS:
    hr_range: FLOAT
        L>> range of heart rate values in data, given in units of beats/minute
    """
    if intype == 'bpm':
        hr_range = max(data) - min(data)
    elif intype == 'ms':
        hr = pd.Series(data).apply(lambda x: 60 / (x / 1000))
        hr_range = max(hr) - min(hr)
    elif intype == 's':
        hr = pd.Series(data).apply(lambda x: 60 / x))
        hr_range = max(hr) - min(hr)
    return hr_range


def compute_rmssd():  # TODO
    """
    DESCRIPTION:
    ----------
    INPUTS (* REQUIRED):
    ----------
    OUTPUTS:
    """
    pass


def compute_hrv_triangular_index():  # TODO
    """
    DESCRIPTION:
    ----------
    INPUTS (* REQUIRED):
    ----------
    OUTPUTS:
    """
    pass


def compute_tinn():  # TODO
    """
    DESCRIPTION:
    ----------
    INPUTS (* REQUIRED):
    ----------
    OUTPUTS:
    """
    pass

# FREQUENCY-DOMAIN METRICS #TODO

# NONLINEAR METRICS #TODO
