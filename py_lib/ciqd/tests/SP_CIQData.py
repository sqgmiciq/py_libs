'''
Created on Aug 9, 2020

@author: wakana_sakashita
'''

if __name__ == '__main__':
    pass

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import datetime
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

    ## Open,close,high,low,volume data 
    def get_market_data(self, company, num) :

        today = datetime.datetime.today()
        delta = int(num[:len(num)-1])

        if num[-1] == 'y':
            start_day = today - relativedelta(years=delta)
        elif num[-1] == 'm':
            start_day = today - relativedelta(months=delta)
        elif num[-1] == 'd':
            start_day = today - datetime.timedelta(days=delta)

        e_day = today.strftime("%Y-%m-%d")
        s_day = start_day.strftime ('%Y-%m-%d')

        
        json = """{ "inputRequests": [
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_OPENPRICE", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_HIGHPRICE", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_LOWPRICE", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_CLOSEPRICE", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_VOLUME", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            ]}""" %(company,s_day,e_day, company,s_day,e_day,company,s_day,e_day,company,s_day,e_day,company,s_day,e_day)
    
        res = self.__call_api(json)
#        print(res)
        if res == 'err' : return -1
        
        SPCIQ = pd.DataFrame()
        for r in res['GDSSDKResponse']:
            if r['NumRows'] > 0:
                if r['Mnemonic'] == 'IQ_OPENPRICE':
                    SPCIQ['Date'] = [pd.to_datetime(x) for x in r['Headers']]
                    SPCIQ['Open'] =  [float(x) for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_HIGHPRICE':
                    SPCIQ['High'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_LOWPRICE':
                    SPCIQ['Low'] = [float(x) for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_CLOSEPRICE':
                    SPCIQ['Close'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_VOLUME':
                    SPCIQ['Volume'] = [float(x) for x in r['Rows'][0]['Row']]   
            
        SPCIQ = SPCIQ.set_index('Date')            

        return SPCIQ
    
    
    
    def get_CreditStatsDirect_KeyStats(self, company, period, date) :
        
        pd.set_option('display.max_columns', None)  
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('max_colwidth', None)
        pd.set_option('display.max_rows', 2000)       
        pd.set_option("display.colheader_justify","left")


        if date :
            today = datetime.datetime.today()
            date = today.strftime("%Y-%m-%d")

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

        SPCIQ = pd.DataFrame()
        for r in res['GDSSDKResponse']:
 #           if r['NumRows'] > 0:
                if r['Mnemonic'] == 'IQ_PERIOD_DATE_CSD': SPCIQ['Period End'] = r['Rows'][0]['Row']  
                elif r['Mnemonic'] == 'IQ_ADJUSTMENT_STATUS_CSD': SPCIQ['Adjustment Status'] = r['Rows'][0]['Row']   
                elif r['Mnemonic'] == 'IQ_REVENUES_ADJ_CSD': SPCIQ['Revenue'] = [float(x) for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_EBITDA_ADJ_CSD': SPCIQ['EBITDA'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_OPERATING_INC_AFTER_DA_ADJ_CSD': SPCIQ['Operating Income (After D&A)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBIT_ADJ_CSD': SPCIQ['EBIT'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FFO_ADJ_PENSIONS_NORMALIZED_CSD': SPCIQ['Funds From Operations (FFO)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_OPS_CASH_FLOW_ADJ_CSD': SPCIQ['Cash flow from operations, Adj.'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_CAPEX_ADJ_CSD': SPCIQ['Capital Expenditures, Adj.'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FREE_OPERATING_CASH_FLOW_ADJ_CSD': SPCIQ['Free Operating Cash Flow'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DIVIDENDS_ADJ_CSD': SPCIQ['Dividends, Adj.'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_Share_Repurchases_CSD': SPCIQ['Share Repurchases'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DISCRETIONARY_CASH_FLOW_ADJ_CSD': SPCIQ['Discretionary Cash Flow'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_CASH_ST_INVESTMENTS_ADJ_CSD': SPCIQ['Cash and Short-Term Investments'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DEBT_ADJ_CSD': SPCIQ['Debt'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EQUITY_ADJ_CSD': SPCIQ['Equity'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DEBT_EQUITY_ADJ_CSD': SPCIQ['Debt and Equity'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DEBT_EBITDA_CSD': SPCIQ['Debt/EBITDA (x)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FFO_DEBT_CSD': SPCIQ['FFO/Debt (%)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_OPS_CASH_FLOW_DEBT_PCT_ADJ_CSD': SPCIQ['Operating Cash Flow/Debt (%)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FREE_OPERATING_CASH_FLOW_TO_DEBT_ADJ_CSD': SPCIQ['Free Operating Cash Flow/Debt (%)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_DISCRETIONARY_CASH_FLOW_DEBT_CSD':SPCIQ['Discretionary Cash Flow/Debt (%)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBITDA_INTEREST_COVERAGE_CSD': SPCIQ['EBITDA Interest Coverage (x)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_FFO_CASH_INTEREST_COVERAGE_CSD' :  SPCIQ['FFO Cash Interest Coverage (x)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBITDA_REVENUES_CSD': SPCIQ['EBITDA Margin (%)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_RETURN_CAPITAL_CSD': SPCIQ['Return on Capital (%)'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_EBIT_MARGIN_CSD':  SPCIQ['EBIT Margin (%)'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_Debt_Debt_Plus_Equity_Underpreciated_Basis_Adj_CSD': SPCIQ['Debt/Debt+Equity(undepreciated basis) (%)'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_Debt_Fixed_Charge_Coverage_Adj_CSD': SPCIQ['Debt Fixed Charge Coverage (%)'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_RETURN_COMMON_EQUITY_ADJ_FOR_AFUDC_CSD': SPCIQ['ROCE (%), Adj. For AFUDC'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_LTV_Adj_CSD': SPCIQ['LTV (%)'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_EBIT_INTEREST_COVERAGE_CSD': SPCIQ['EBIT Interest Coverage (x)'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_ANNUAL_REVENUE_GROWTH_ADJ_CSD': SPCIQ['Revenue Growth (%)'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_OPERATING_INC_AFTER_DA_REVENUES_CSD': SPCIQ['Operating Income (After D&A)/Revenues (%)'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_COMMON_DIVIDEND_PAYOUT_RATIO_CSD': SPCIQ['Common Dividend Payout Ratio (%)'] = [float(x) for x in r['Rows'][0]['Row']]
                elif r['Mnemonic'] == 'IQ_LAST_UPDATED_CSD': SPCIQ['Last Update Date'] = r['Rows'][0]['Row']

        SPCIQ = SPCIQ.set_index('Period End')            

        return SPCIQ


