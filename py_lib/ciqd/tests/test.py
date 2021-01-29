'''
Created on Aug 10, 2020

@author: wakana_sakashita
'''
#import spciqdata
#from spciqdata import SP_CIQData

import ciqd.tests.SP_CIQData as SP_CIQData

ciq = SP_CIQData.ciq("wakana.sakashita@spglobal.com", "HakubaSki!21")
#SP = ciq.get_market_data("AAPL:", '5y')
SP = ciq.get_CreditStatsDirect_KeyStats("AAPL:", "IQ_FY", "2020/09/07")

print(SP)
