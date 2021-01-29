'''
Created on Aug 9, 2020

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
    
    
    # CSD KeyStats data
    # -- required parameters 1 - company : IQ number, ticker, CUSIP, ISIN, SEDOOL..,  2 - period : IQ_FY,IQ_FY-1... IQ_CY, IQ_LTM.. 
    # -- optional parameter  3 - date : yyyy-mm-dd format, if not specified, today's date will be used
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
    def get_CreditStatsDirect_KeyStats(self, company, period, date=datetime.today().strftime("%Y-%m-%d")) :
        

        json = """{{ "inputRequests": [
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_PERIOD_DATE_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
#            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_ADJUSTMENT_STATUS_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_REVENUES_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBITDA_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OPERATING_INC_AFTER_DA_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBIT_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FFO_ADJ_PENSIONS_NORMALIZED_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OPS_CASH_FLOW_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_CAPEX_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FREE_OPERATING_CASH_FLOW_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DIVIDENDS_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
#            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_Share_Repurchases_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DISCRETIONARY_CASH_FLOW_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_CASH_ST_INVESTMENTS_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DEBT_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DEBT_EQUITY_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EQUITY_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DEBT_EBITDA_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FFO_DEBT_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OPS_CASH_FLOW_DEBT_PCT_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FREE_OPERATING_CASH_FLOW_TO_DEBT_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DISCRETIONARY_CASH_FLOW_DEBT_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBITDA_INTEREST_COVERAGE_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FFO_CASH_INTEREST_COVERAGE_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBITDA_REVENUES_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_RETURN_CAPITAL_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBIT_MARGIN_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
#            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_Debt_Debt_Plus_Equity_Underpreciated_Basis_Adj_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
#            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_Debt_Fixed_Charge_Coverage_Adj_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_RETURN_COMMON_EQUITY_ADJ_FOR_AFUDC_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
#            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_LTV_Adj_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBIT_INTEREST_COVERAGE_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_ANNUAL_REVENUE_GROWTH_ADJ_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OPERATING_INC_AFTER_DA_REVENUES_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_COMMON_DIVIDEND_PAYOUT_RATIO_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_LAST_UPDATED_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
        ]}}""".format(company,period,date)
        
        res = self.__call_api(json)
#        print(res)
        if res == 'err' : return -1

        df = pd.DataFrame()
        for r in res['GDSSDKResponse']:
            if r['NumRows'] > 0:
                if r['Mnemonic'] == 'IQ_PERIOD_DATE_CSD': df['Period End'] = r['Rows'][0]['Row']  
                elif r['Mnemonic'] == 'IQ_ADJUSTMENT_STATUS_CSD': df['Adjustment Status'] = r['Rows'][0]['Row']   
                elif r['Mnemonic'] == 'IQ_REVENUES_ADJ_CSD': df['Revenue'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_EBITDA_ADJ_CSD': df['EBITDA'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_OPERATING_INC_AFTER_DA_ADJ_CSD': df['Operating Income (After D&A)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBIT_ADJ_CSD': df['EBIT'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FFO_ADJ_PENSIONS_NORMALIZED_CSD': df['Funds From Operations (FFO)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_OPS_CASH_FLOW_ADJ_CSD': df['Cash flow from operations, Adj.'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_CAPEX_ADJ_CSD': df['Capital Expenditures, Adj.'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FREE_OPERATING_CASH_FLOW_ADJ_CSD': df['Free Operating Cash Flow'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DIVIDENDS_ADJ_CSD': df['Dividends, Adj.'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_Share_Repurchases_CSD': df['Share Repurchases'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DISCRETIONARY_CASH_FLOW_ADJ_CSD': df['Discretionary Cash Flow'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_CASH_ST_INVESTMENTS_ADJ_CSD': df['Cash and Short-Term Investments'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DEBT_ADJ_CSD': df['Debt'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EQUITY_ADJ_CSD': df['Equity'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DEBT_EQUITY_ADJ_CSD': df['Debt and Equity'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DEBT_EBITDA_CSD': df['Debt/EBITDA (x)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FFO_DEBT_CSD': df['FFO/Debt (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_OPS_CASH_FLOW_DEBT_PCT_ADJ_CSD': df['Operating Cash Flow/Debt (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FREE_OPERATING_CASH_FLOW_TO_DEBT_ADJ_CSD': df['Free Operating Cash Flow/Debt (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DISCRETIONARY_CASH_FLOW_DEBT_CSD':df['Discretionary Cash Flow/Debt (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBITDA_INTEREST_COVERAGE_CSD': df['EBITDA Interest Coverage (x)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FFO_CASH_INTEREST_COVERAGE_CSD' :  df['FFO Cash Interest Coverage (x)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBITDA_REVENUES_CSD': df['EBITDA Margin (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_RETURN_CAPITAL_CSD': df['Return on Capital (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBIT_MARGIN_CSD':  df['EBIT Margin (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_Debt_Debt_Plus_Equity_Underpreciated_Basis_Adj_CSD': df['Debt/Debt+Equity(undepreciated basis) (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_Debt_Fixed_Charge_Coverage_Adj_CSD': df['Debt Fixed Charge Coverage (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_RETURN_COMMON_EQUITY_ADJ_FOR_AFUDC_CSD': df['ROCE (%), Adj. For AFUDC'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_LTV_Adj_CSD': df['LTV (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_EBIT_INTEREST_COVERAGE_CSD': df['EBIT Interest Coverage (x)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_ANNUAL_REVENUE_GROWTH_ADJ_CSD': df['Revenue Growth (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_OPERATING_INC_AFTER_DA_REVENUES_CSD': df['Operating Income (After D&A)/Revenues (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_COMMON_DIVIDEND_PAYOUT_RATIO_CSD': df['Common Dividend Payout Ratio (%)'] = [float(x) if not x == 'Data Unavailable' else x  for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_LAST_UPDATED_CSD': df['Last Update Date'] = r['Rows'][0]['Row']


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
    def get_Financials_IncomeStatement(self, company, period, date=datetime.today().strftime("%Y-%m-%d")) :
        

        json = """{{ "inputRequests": [
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_REV", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FIN_DIV_REV", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_INS_DIV_REV", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_GAIN_ASSETS_REV", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_GAIN_INVEST_REV", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_INT_INV_INC", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OTHER_REV_SUPPL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_TOTAL_REV", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_COGS", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FIN_DIV_EXP", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_INS_DIV_EXPD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FIN_DIV_INT_EXP", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_GP", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_SGA_SUPPL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EXPLORE_DRILL, "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_PROV_BAD_DEBTS", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_STOCK_BASED", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DEBT_EBITDA_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_FFO_DEBT_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_PRE_OPEN_COST", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_RD_EXP", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_DA_SUPPL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_GW_INTAN_AMORT", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_IMPAIR_OIL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OTHER_OPER", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_RETURN_CAPITAL_CSD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OPER_INC", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_INTEREST_EXP", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_INTEREST_INVEST_INC", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_NET_INTEREST_EXP", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_INC_EQUITY", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_CURRENCY_GAIN", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OTHER_NON_OPER_EXP_SUPPL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBT_EXCL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_RESTRUCTURE", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_MERGER", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_IMPAIRMENT_GW", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_GAIN_INVEST", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_GAIN_ASSETS", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_ASSET_WRITEDOWN", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_IPRD", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_INS_SETTLE", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_LEGAL_SETTLE", "properties":{{"perviodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_OTHER_UNUSUAL_SUPPL", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "IQ_EBT", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
            {{"function": "GDSP", "identifier": "{0}", "mnemonic": "", "properties":{{"periodType": "{1}", "asofDate": "{2}"}}}},
        ]}}""".format(company,period,date)
        
        res = self.__call_api(json)
#        print(res)
        if res == 'err' : return -1

        df = pd.DataFrame()
        for r in res['GDSSDKResponse']:
            if r['NumRows'] > 0:
                if r['Mnemonic'] == 'IQ_PERIOD_DATE_CSD': df['Period End'] = r['Rows'][0]['Row']  

        return df