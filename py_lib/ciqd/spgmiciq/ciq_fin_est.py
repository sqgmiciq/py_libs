'''
Created on Jan 15, 2021

@author: wakana_sakashita
'''

if __name__ == '__main__':
    pass

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


url = "https://api-ciq.marketintelligence.spglobal.com/gdsapi/rest/v3/clientservice.json"
headers = {'Content-Type': 'application/json'}


class ciq:
    def __init__(self, user, pwd):
        self.user = user
        self.pwd = pwd


    def __call_api(self, json):
    
        resp = requests.post(url, auth=HTTPBasicAuth(self.user, self.pwd), headers=headers, data=json)
    
    
        if(resp.status_code >= 400 and resp.status_code < 600):
            print("HTTP error %d is returned during the API process." %(resp.status_code))
            print(resp.text)
            return 'err'

        data = resp.json()

        if(isinstance(data, dict) is False):
            print("HTTP error %d is returned during the API process." %(resp.status_code))
            print(resp.text)
            return 'err'

        elif('Errors' in data.keys()):
            print("Error occurred during the API process : %s" %(data))
            return 'err'

        return data

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
    ## Open,close,high,low,volume data 
    # -- required parameters 1 - company : IQ number, ticker, CUSIP, ISIN, SEDOOL..,  2 - num : histrical duration ** y-Year, m-Month, d-Day, ex.'10y', '6m', '100d'
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
    def get_market_data(self, company, num) :

        today = datetime.now()
        delta = int(num[:len(num)-1])

        if num[-1] == 'y':
            start_day = today - relativedelta(years=delta)
        elif num[-1] == 'm':
            start_day = today - relativedelta(months=delta)
        elif num[-1] == 'd':
            start_day = today - timedelta(days=delta)

        e_day = today.strftime("%Y-%m-%d")
        s_day = start_day.strftime ('%Y-%m-%d')

        
        json = """{{ "inputRequests": [
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_OPENPRICE", "properties":{{"startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_HIGHPRICE", "properties":{{"startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_LOWPRICE", "properties":{{"startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_CLOSEPRICE", "properties":{{"startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_VOLUME", "properties":{{"startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            ]}}""".format(company,s_day,e_day)
    
        res = self.__call_api(json)
#        print(res)
        if res == 'err' : return -1
        
        df = pd.DataFrame()
        for r in res['GDSSDKResponse']:
            if r['NumRows'] > 0:
                if r['Mnemonic'] == 'IQ_OPENPRICE':
                    df['Date'] = [pd.to_datetime(x) for x in r['Headers']]
                    df['Open'] =  [float(x) for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_HIGHPRICE':
                    df['High'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_LOWPRICE':
                    df['Low'] = [float(x) for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_CLOSEPRICE':
                    df['Close'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_VOLUME':
                    df['Volume'] = [float(x) for x in r['Rows'][0]['Row']]   
            

        return df
    
    
  

    # CIQ Financial Income Statement
    # -- required parameters 1 - company : IQ number, ticker, CUSIP, ISIN, SEDOOL..,  2 - period : IQ_FY,IQ_FY-1... IQ_CY, IQ_LTM.. 
    # -- optional parameter  3 - date : yyyy-mm-dd format, if not specified, today's date will be used
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
    def get_Financials_Multiples(self, company, startdate=datetime.today().strftime("%Y-%m-%d"), enddate=datetime.today().strftime("%Y-%m-%d")) :

        json = """{{ "inputRequests": [
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_TEV_TOTAL_REV", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_TEV_TOTAL_REV_FWD", "properties":{{"periodType": "IQ_NTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_TEV_EBITDA", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_TEV_EBITDA_FWD", "properties":{{"periodType": "IQ_NTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_TEV_EBIT", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_TEV_EBIT_FWD", "properties":{{"periodType": "IQ_NTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_PE_EXCL", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_PE_EXCL_FWD", "properties":{{"periodType": "IQ_NTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_PE_NORMALIZED", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_PBV", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_PBV_FWD", "properties":{{"periodType": "IQ_NTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_PTBV", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_PRICE_CFPS_FWD", "properties":{{"periodType": "IQ_NTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_TEV_UFCF", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_MARKET_CAP_LFCF", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_MKTCAP_TOTAL_REV", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_MKTCAP_TOTAL_REV_FWD", "properties":{{"periodType": "IQ_NTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_MKTCAP_EBT_EXCL", "properties":{{"periodType": "IQ_LTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},
            {{"function": "GDST", "identifier": "{0}", "mnemonic": "IQ_PEG_FWD", "properties":{{"periodType": "IQ_NTM","startDate": "{1}", "endDate": "{2}", "frequency": "Daily"}}}},

        ]}}""".format(company,startdate,enddate)
        
        res = self.__call_api(json)
#        print(res)
        if res == 'err' : return -1

        df = pd.DataFrame()
        for r in res['GDSSDKResponse']:
            if r['NumRows'] > 0 :
                if r['Mnemonic'] == 'IQ_TEV_TOTAL_REV': 
                    df['Date'] = [pd.to_datetime(x) for x in r['Headers']]
                    df['TEV/LTM Total Revenue'] =  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_TEV_TOTAL_REV_FWD': df['TEV/NTM Total Revenues'] = [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_TEV_EBITDA': df['TEV/LTM EBITDA'] =  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_TEV_EBITDA_FWD': df['TEV/NTM EBITDA'] =  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_TEV_EBIT': df['TEV/LTM EBIT'] =  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_TEV_EBIT_FWD': df['TEV/NTM EBIT']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_PE_EXCL': df['Price / LTM EPS']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_PE_EXCL_FWD': df['Price / NTM EPS']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_PE_NORMALIZED': df['Price / LTM Normalized EPS']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_PBV': df['Price / Book Value']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_PBV_FWD': df['Forward P / BV']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_PTBV': df['Price / Tangible Book Value']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_PRICE_CFPS_FWD': df['Price / NTM CFPS']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_TEV_UFCF': df['TEV / LTM Unlevered FCF']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_MARKET_CAP_LFCF': df['Market Cap / LTM Levered FCF']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_MKTCAP_TOTAL_REV': df['Market Cap / LTM Total Revenues']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_MKTCAP_TOTAL_REV_FWD': df['Market Cap / Forward Total Revenues']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_MKTCAP_EBT_EXCL': df['Market Cap / LTM EBT Excl Unusual Items']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_PEG_FWD': df['PEG Ratio']=  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  

        return df


    # CIQ Financial Income Statement
    # -- required parameters 1 - company : IQ number, ticker, CUSIP, ISIN, SEDOOL..,  2 - period : IQ_FY,IQ_FY-1... IQ_CY, IQ_LTM.. 
    # -- optional parameter  3 - date : yyyy-mm-dd format, if not specified, today's date will be used
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
    def get_KeyStats_Fin_Est(self, company, period, date=datetime.today().strftime("%Y-%m-%d")) :
        

        json = """{{ "inputRequests": [
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_PERIODDATE", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_TOTAL_REV", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_NI", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_GP", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBITDA", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EARNING_CO", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DILUT_EPS_EXCL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_CASH_OPER", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_TOTAL_ASSETS", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_TOTAL_CA", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "Q_TOTAL_CL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_BV_SHARE", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_PRICE_TARGET_CIQ", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EST_EPS_GROWTH_5YR_CIQ", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EPS_NORM_EST", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
        ]}}""".format(company,period,date)
        
        res = self.__call_api(json)
#        print(res)
        if res == 'err' : return -1

        df = pd.DataFrame()

        for r in res['GDSSDKResponse']:
            if r['NumRows'] > 0:
                if r['Mnemonic'] == 'IQ_PERIODDATE': 
                    df['Period Date'] = [pd.to_datetime(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_TOTAL_REV': df['Total Revenue'] =  [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_NI': df['Net Income'] = [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_GP': df['Gross Profit'] = [float(x) if not x == 'Data Unavailable' else x for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBITDA': df['EBITDA'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_EARNING_CO': df['Earnings from Continuous Operation'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DILUT_EPS_EXCL': df['Diluted EPS Excl Extra'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_CASH_OPER': df['Cash from Ops'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_TOTAL_ASSETS': df['Total Assets'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_TOTAL_CA': df['Total Current Assets'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_TOTAL_CL': df['Total Current Liabilities'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_BV_SHARE': df['Book Value/share'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_PRICE_TARGET_CIQ': df['Target Price'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EST_EPS_GROWTH_5YR_CIQ': df['Long Term Growth'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EPS_NORM_EST': df['EPS Normalized'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   

        return df