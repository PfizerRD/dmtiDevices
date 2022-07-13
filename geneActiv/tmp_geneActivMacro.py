import pandas as pd

# This script is to generate the GENEActiv macros in python - Pythonic implimentation of the macros

if __name__ == '__main__':
    file = '/Users/psaltd/Downloads/OneDrive_1_2-28-2022/gas1047_set2_wrist_052073_geneactiv_epoch60s.csv'
    df = pd.read_csv(file, header=None)
    df.head()
    header = df.iloc[0:101, :]
    data =  df.iloc[101:, :]
    timestamps = pd.to_datetime(data.iloc[:,0])
    totalDays = (timestamps.iloc[-1] - timestamps.iloc[0])
    #check data shape
    # If DataLength > 0 And DataLength < 1440 Then
    #     msg = MsgBox("The data is less than 1 day in length, more than one day is required for calculation of charts and tables", vbCritical, "Data Import error")
    if data.shape[0] < 1440:
        print('One day of data not present')
        raise ValueError
    elif data.shape[0] == 0:
        print('no data present')
        raise ValueError
    else:
        pass



