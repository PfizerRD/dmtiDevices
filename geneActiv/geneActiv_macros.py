import xlwings as xw
import pandas as pd
import os
import time
import sys
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/')
from tqdm import tqdm
import argparse
#from helpers import get_gaMacro_files_AWS, downloads

def geneactiv_everyday_living_macro_csv(filename, save_name):
    path = "/Users/bluesky/Desktop/GeneActiv_Macros/"
    #path = '/Users/psaltd/Desktop/Geneactiv_macro/'
    #macro = path + 'GENEActiv_Everyday_Living_Overview_1.11_IK_test_withModule1_update.xlsm'
    macro = path + 'GENEActiv_Everyday_Living_Overview_1.11_IK_test_withModule1.xlsm'

    wb = xw.Book(macro)
    time.sleep(5)
    #Import data
    task1 = "ImportDataFile"
    ExcelMacro = wb.macro(task1)
    app = xw.apps.active
    app.DisplayAlerts = False

    ExcelMacro.run(filename)

    # Generate Plots
    task2 = 'Data_analysis'
    genplots = wb.macro(task2)
    genplots.run()

    full_data = wb.sheets[1]

    full_data.book.save(save_name)
    app.quit()

    time.sleep(10)

def geneactiv_sleep_macro_csv(filename, save_name):
    path = '/Users/bluesky/Desktop/GeneActiv_Macros/'
    #path = '/Users/psaltd/Desktop/Geneactiv_macro/'
    macro = path + 'GENEActiv_Sleep_Macro_5.xlsm'

    wb = xw.Book(macro)
    time.sleep(5)
    # Import data
    task1 = "ImportDataFile"
    ExcelMacro = wb.macro(task1)
    app = xw.apps.active
    app.DisplayAlerts = False
    ExcelMacro.run(filename)

    # Generate Plots
    task2 = 'Data_analysis'
    genplots = wb.macro(task2)
    genplots.run()

    full_data = wb.sheets[1]
    full_data.book.save(save_name)
    time.sleep(15)
    app.quit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process GA Macros - Cachexia 1009 Study')
    parser.add_argument('--macrotype', type=str, default='Everyday_living_macro',
                        help='enter the type of macro (Everyday_living_macro or Sleep_macro)')
    parser.add_argument('--download', type=bool, default=False,
                        help='Set to True to download new 60s epoch files from AWS')
    parser.add_argument('--position', type=str, default='wrist',
                        help='position of GA for macro processing (wrist or lumbar)')
    args = parser.parse_args()

    #to download new files
    if args.download == True:
        ga_file = get_gaMacro_files_AWS()
        [downloads(x.s3_obj) for y, x in tqdm(ga_file.iterrows())]
    else:
        pass

    #path = '/Users/psaltd/Desktop/Cachexia/data/C3651009/processed/geneactiv/epoch60s/'
    #path = '/Users/bluesky/Desktop/Cachexia/data/C3651009/processed/geneactiv/epoch60s/'
    path = '/Volumes/Promise_Pegasus/cachexia_1009/C3651009/processed/geneactiv/epoch60s/'
    files = os.listdir(path)
    save_path = '/Users/bluesky/Desktop/Cachexia/processed/'
    #########
    macrotype = args.macrotype
    #########

    for f in tqdm(files):
        #Isik does not want lumbar files
        if not args.position in f:
            continue
        ##for debugging
        if f == '10031001_screening_wrist_GA_60sEpoch.csv': #errored file
            continue
        [subject, visit, devloc, toss, toss2] = f.split('_')
        save_name = '{}_{}_{}_{}_processed.csv'.format(subject, visit, devloc, macrotype)
        if os.path.exists(save_path+save_name):
            continue
        else:
            print('{} is being processed...'.format(f))
            if macrotype == 'Sleep_macro':
                geneactiv_sleep_macro_csv(path + f, save_path + save_name)
            if macrotype == 'Everyday_living_macro':
                geneactiv_everyday_living_macro_csv(path + f, save_path + save_name)
        time.sleep(3)

    print('Done!')