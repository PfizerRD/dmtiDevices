'''
from src.APDM.balance.APDM_balance_updated import *
from src.APDM.gait.APDM_Gait_features_all_metrics import *
from src.APDM.s2s.APDM_s2s_features import *
from src.APDM.TUG.APDM_TUG_features import *
'''
import sys
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/APDM/balance/') #path where the file is downloaded
from APDM_balance_updated import *
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/APDM/gait/') #path where the file is downloaded
from APDM_Gait_features_all_metrics import *
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/APDM/s2s/') #path where the file is downloaded
from APDM_s2s_features import *
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/APDM/TUG/')
from APDM_TUG_features import *

def APDM_main():
    run_STEPP_TUG_APDM()
    run_STEPP_balance_APDM()
    run_STEPP_APDM_gait()
    run_STEPP_s2s_APDM()

    print('Done!')

if __name__ == '__main__':
    APDM_main()