from requests.auth import HTTPBasicAuth
from datetime import timedelta as td
from datetime import datetime as dt
import pandas as pd
import requests
import quandl
import time

def initiate_API(live=False):
  return Alpaca(live)

class Alpaca(object):
  def __init__(self,
               live:bool = False,
               key: str = None,
               secret: str = None,
               base_url: str = None,
               data_url: str = None,
               crypto_url: str = None,
               ):

    self._auth = HTTPBasicAuth(key, secret)
    self._base_url = base_url
    self._data_url = data_url
    self._crypto_url = crypto_url
    self._account_id = None
    

  def _get_account_id(self):
    if self._account_id == None:
      raise Exception('An account ID has not yet been set.')
    return self._account_id

  def _set_account_id(self,value:str = None):
    if not isinstance(value, str):
      raise Exception('Account ID must be of type str.')
    if len(value) != 36:
      raise Exception('Account ID must be 36 characters long.')
    self._account_id = value

  def _set_id(self,id:str):
    if id == None:
      if self._account_id != None:
        id = self.account_id
      else:
        raise Exception('No account ID was specified. Either set the account_id property or pass an account_id to a method directly.')
    return id

  def _check_status(self,response = None):
    code = str(response.status_code)
    if code[0] != '2':
      raise Exception('API call was unsuccesful per the following: Status Code Error ' + code + ' ' + response.text)

  account_id = property(_get_account_id,_set_account_id)

  def get_all_accounts(self):
    url = self._base_url + 'accounts'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_brokerage_account(self,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'accounts/' + id
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def patch_brokerage_account(self,data:dict = None,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'accounts/' + id
    r = requests.patch(url=url,params=data,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_trading_account(self,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'trading/accounts/' + id + '/account'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_account_equity(self,id:str = None):
    id = self._set_id(id)
    return float(self.get_trading_account(id)['equity'])

  def get_activities(self,activity_type:str = None,date:str=None,id:str = None):
    id = self._set_id(id)
    params = {
              'after':date,
              'account_id':id,
              'page_size':10000
    }

    if activity_type == None:
      url = self._base_url + 'accounts/activities'
    else:
      url = self._base_url + 'accounts/activities/' + activity_type
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_portfolio_history(self,id:str = None):
    id = self._set_id(id)

    params = {'period':'1A',
              'timeframe':'1D'}

    url = self._base_url + 'trading/accounts/' + id + '/account/portfolio/history'
    r = requests.get(url=url,params=params,auth=self._auth)
    self._check_status(r)

    history = pd.DataFrame(r.json())
    history['timestamp'] = pd.to_datetime(history.timestamp,unit='s')
    history = history.set_index('timestamp').sort_values('timestamp',ascending=False)
    return history

  def get_prev_mth_avg_equity(self,id:str = None):
    id = self._set_id(id)

    now = dt.utcnow()
    end = now.replace(day=1) - td(days=1)
    start = now.replace(day=1) - td(days=end.day)
    mth_end = end.strftime('%Y-%m-%d')
    mth_start = start.strftime('%Y-%m-%d')
    acc_start = pd.Timestamp(self.get_brokerage_account(id)['created_at']).strftime('%Y-%m-%d')

    history = self.get_portfolio_history(id)
    prev_mth = history[(history.index >= mth_start) & (history.index <= mth_end)]

    if acc_start > mth_start:
      pd.options.mode.chained_assignment = None
      prev_mth.loc[(prev_mth.index <= acc_start),'equity'] = 0.0
      pd.options.mode.chained_assignment = 'warn'
      
    avg_equity = float(prev_mth.equity.mean())

    return avg_equity

  def get_account_ids(self,
                      active:bool = None,
                      funded:bool = None):
      
    #Returns all active account IDs.
    if active == True and funded == None:
      ids = [x['id'] for x in self.get_all_accounts() if x['status'] == 'ACTIVE']
    #Returns all active and funded account IDs.
    elif active == True and funded == True:
      ids = [x['id'] for x in self.get_all_accounts() if (x['status'] == 'ACTIVE') and (float(self.get_trading_account(x['id'])['equity']) > 0)]
    #Returns all funded account IDs.
    elif active == None and funded == True:
      ids = [x['id'] for x in self.get_all_accounts() if (float(self.get_trading_account(x['id'])['equity']) > 0)]
    #Returns all active account IDs with zero or negative funds.
    elif active == True and funded == False:
      ids = [x['id'] for x in self.get_all_accounts() if (x['status'] == 'ACTIVE') and (float(self.get_trading_account(x['id'])['equity']) <= 0)]
    #Returns all closed account IDs.
    elif active == False and funded == None:
      ids = [x['id'] for x in self.get_all_accounts() if x['status'] == 'ACCOUNT_CLOSED']
    #Returns all closed account IDs with zero or negative funds.
    elif active == False and funded == False:
      ids = [x['id'] for x in self.get_all_accounts() if (x['status'] == 'ACCOUNT_CLOSED') and (float(self.get_trading_account(x['id'])['equity']) <= 0)]
    #Returns all account IDs with zero or negative funds.
    elif active == None and funded == False:
      ids = [x['id'] for x in self.get_all_accounts() if (float(self.get_trading_account(x['id'])['equity']) <= 0)]
    #Returns all closed accounts that are still funded.
    elif active == False and funded == True:
      ids = [x['id'] for x in self.get_all_accounts() if (x['status'] == 'ACCOUNT_CLOSED') and (float(self.get_trading_account(x['id'])['equity']) > 0)]
    #Return all account IDs
    elif active == None and funded == None:
      ids = [x['id'] for x in self.get_all_accounts()]
    else:
      raise Exception('Was not able to retrieve IDs based on specified filters.')

    return ids

  def get_account_configs(self,id:str = None):
    id = self._set_id(id)
    url = self._base_url + '/trading/accounts/' + id + '/account/configurations'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def patch_account_configs(self,
                            dtbp_check:str = 'entry',
                            fractional_trading:bool = True,
                            max_margin_multiplier:float = 2.0,
                            no_shorting:bool = True,
                            pdt_check:str = 'entry',
                            suspend_trade:bool = False,
                            trade_confirm_email:str = 'none',
                            id:str = None,
                            ):
    
    data = {'dtbp_check': dtbp_check,
            'fractional_trading': fractional_trading,
            'max_margin_multiplier': max_margin_multiplier,
            'no_shorting': no_shorting,
            'pdt_check': pdt_check,
            'suspend_trade': suspend_trade,
            'trade_confirm_email': trade_confirm_email}

    id = self._set_id(id)
    url = self._base_url + '/trading/accounts/' + id + '/account/configurations'
    r = requests.patch(url=url,json=data,auth=self._auth)
    self._check_status(r)
    return r.json()

  def submit_order(self,
                   symbol:str = None,
                   qty:str = None,
                   side:str = None,
                   id:str = None,):
    
    if self.get_asset_class(symbol) == 'crypto': tif = 'gtc'
    else: tif = 'day'
    
    data = {'symbol':str(symbol),
            'qty':str(qty),
            'side':str(side),
            'type':'market',
            'time_in_force':tif}

    id = self._set_id(id)
    url = self._base_url + 'trading/accounts/' + id + '/orders'
    r = requests.post(url=url,json=data,auth=self._auth)
    if str(r.status_code) == '403':
      message = 'The ' + side + ' order for ' + qty + ' shares of ' + symbol + ' was skipped because of the following error:'
      return print(message,r.json())
    else:
      self._check_status(r)
      order = r.json()
      message = 'A ' + order['side'] + ' order for ' + order['qty'] + ' shares of ' + order['symbol'] + ' was submitted at ' + order['submitted_at'] + '.'
      return print(message) 

  def get_all_orders(self,status:str='open',date=pd.Timestamp('2022-01-01'),id:str = None):
    params = {'status':status,
              'limit':500,
              'after':date}
    id = self._set_id(id)
    url = self._base_url + 'trading/accounts/' + id + '/orders'
    r = requests.get(url=url,params=params,auth=self._auth)
    self._check_status(r)
    return r.json()

  def cancel_all_orders(self,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'trading/accounts/' + id + '/orders'
    r = requests.delete(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_all_positions(self,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'trading/accounts/' + id + '/positions'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def close_all_positions(self,id:str = None):
    id = self._set_id(id)
    params = {'cancel_orders':True}
    url = self._base_url + 'trading/accounts/' + id + '/positions'
    r = requests.delete(url=url,params=params,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_positions_df(self,id:str = None):
    id = self._set_id(id)
    positions = [[x['symbol'],x['qty'],x['side'],x['market_value'],x['asset_class']] for x in self.get_all_positions(id)]
    positions = pd.DataFrame(positions,columns=['symbol','qty','side','market_value','asset_class']).set_index('symbol') 
    return positions

  def get_position(self,symbol:str = None,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'trading/accounts/' + id + '/positions/' + symbol
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def close_position(self,symbol:str = None,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'trading/accounts/' + id + '/positions/' + symbol
    r = requests.delete(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_assets(self,asset_class:str = 'us_equity'):

    params = {'asset_class':asset_class}
    
    url = self._base_url + 'assets'
    r = requests.get(url=url,params=params,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_asset(self,symbol:str = None):
    url = self._base_url + 'assets/' + symbol
    params = {'symbol_or_asset_id':symbol}
    r = requests.get(url=url,params=params,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_asset_class(self,symbol:str = None):
    return self.get_asset(symbol)['class']

  def get_asset_logo(self,symbol:str = None):
    url = self._logo_url + symbol
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r

  def get_last_quote(self,symbol:str = None):
    asset_class = self.get_asset(symbol)['class']
    if asset_class == 'crypto':
      if '/' not in symbol:
        symbol = symbol.split('USD')[0] + '/USD'
      url = self._crypto_url + 'latest/quotes'
      params = {'symbols':symbol}
      r = requests.get(url=url,params=params,auth=self._auth)
      self._check_status(r)
      return r.json()['quotes'][symbol]
    else:
      url = self._data_url + 'stocks/' + symbol + '/quotes/latest'
      r = requests.get(url=url,auth=self._auth)
      self._check_status(r)
      return r.json()['quote']


  def get_last_trade(self,symbol:str = None):
    asset_class = self.get_asset(symbol)['class']
    if asset_class == 'crypto':
      if '/' not in symbol:
        symbol = symbol.split('USD')[0] + '/USD'
      url = self._crypto_url + 'latest/trades'
      params = {'symbols':symbol}
      r = requests.get(url=url,params=params,auth=self._auth)
      self._check_status(r)
      return r.json()['trades'][symbol]
    else:
      url = self._data_url + 'stocks/' + symbol + '/trades/latest'
      r = requests.get(url=url,auth=self._auth)
      self._check_status(r)
      return r.json()['trade']

  def get_clock(self):
    url = self._base_url + 'clock'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_calendar(self):
    url = self._base_url + 'calendar'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def is_trading_day(self,schedule='daily'):

      df = pd.DataFrame(self.get_calendar())
      df['key'] = df.date.apply(lambda x:x[:-3])
      today = pd.Timestamp(dt.utcnow().date())

      if schedule == 'daily':
          trading_days = pd.to_datetime(df.date).tolist()
      
      elif schedule == 'month_start':
          trading_days = pd.to_datetime(df.groupby('key').min().date).tolist()

      elif schedule == 'month_end':
          trading_days = pd.to_datetime(df.groupby('key').max().date).tolist()

      else:
          raise Exception('Schedule is not properly specified.')

      boolean = today in trading_days
      return boolean

  def get_asset_volatility(self,symbol:str = None,lookback:int=63):

    quandl.ApiConfig.api_key = "tZowwzzJLTGsuZBrTAyq"
    asset_class = self.get_asset_class(symbol)
    if asset_class == 'crypto':
      raise Exception('Use FTX API for crypto pairs.')

    if lookback == 'MAX':
      start = '1990-01-01'
      df = quandl.get_table('SHARADAR/SFP', date={'gte':start}, ticker=symbol)
      return (df.sort_values('date').closeadj.pct_change(1).rolling(len(df.index.values) - 1).std()*(252**0.5)).iloc[-1]
    else:
      start = (dt.today() - pd.Timedelta(days=lookback*2)).strftime("%Y-%m-%d")
      df = quandl.get_table('SHARADAR/SFP', date={'gte':start}, ticker=symbol)
      if len(df.index.values) < (lookback + 1):
        return (df.sort_values('date').closeadj.pct_change(1).rolling(len(df.index.values) - 1).std()*(252**0.5)).iloc[-1]
      else:
        return (df.sort_values('date').closeadj.pct_change(1).rolling(lookback).std()*(252**0.5)).iloc[-1]

  def get_asset_momentum(self,symbol:str = None,lookback:int=252):

    quandl.ApiConfig.api_key = "tZowwzzJLTGsuZBrTAyq"
    asset_class = self.get_asset_class(symbol)
    if asset_class == 'crypto':
      raise Exception('Use FTX API for crypto pairs.')

    start = (dt.today() - pd.Timedelta(days=lookback*2)).strftime("%Y-%m-%d")
    df = quandl.get_table('SHARADAR/SFP', date={'gte':start}, ticker=symbol)
    if len(df.index.values) < (lookback + 1):
      returns = (df.sort_values('date').closeadj.pct_change(len(df.index.values) - 1)).iloc[-1]
      volatility = (df.sort_values('date').closeadj.pct_change(1).rolling(len(df.index.values) - 1).std()*(252**0.5)).iloc[-1]
      sharpe_ratio = (returns/volatility)
      return sharpe_ratio
    else:
      returns = (df.sort_values('date').closeadj.pct_change(lookback)).iloc[-1]
      volatility = (df.sort_values('date').closeadj.pct_change(1).rolling(lookback).std()*(252**0.5)).iloc[-1]
      sharpe_ratio = (returns/volatility)
      return sharpe_ratio

  def get_price_history(self,symbol:str = None,lookback:int=63):

    start = (dt.today() - pd.Timedelta(days=lookback)).strftime("%Y-%m-%d")
    end = (dt.today() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    timeframe = '1Day'
    exchanges = ['CBSE']

    asset_class = self.get_asset(symbol)['class']

    if asset_class == 'crypto':
      url = self._crypto_url + symbol + '/bars'
      params = {'start':start,'end':end,'timeframe':timeframe,'exchanges':exchanges}
    else:
      url = self._data_url + 'stocks/' + symbol + '/bars'
      params = {'start':start,'end':end,'timeframe':timeframe}
    
    r = requests.get(url=url,params=params,auth=self._auth)
    self._check_status(r)
    history = r.json()
    df = pd.DataFrame(history['bars'])
    return df

  def create_new_ach(self,bank_name:str = 'Test Bank',account_number:str = '123456789',routing_number:str = '123456789',checking:bool = True,id:str = None):

    id = self._set_id(id)

    identity = self.get_brokerage_account()['identity']
    first = identity['given_name']
    last = identity['family_name']
    client_name = first + ' ' + last

    if checking:
      account_type = 'CHECKING'
    else:
      account_type = 'SAVINGS'

    nickname = bank_name + ' ' + account_type.capitalize()

    data = {'account_owner_name': client_name,
            'bank_account_type': account_type,
            'bank_account_number': account_number,
            'bank_routing_number': routing_number,
            'nickname': nickname}

    url = self._base_url + 'accounts/' + id + '/ach_relationships'
    r = requests.post(url=url,json=data,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_ach_data(self,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'accounts/' + id + '/ach_relationships'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def delete_ach(self,ach_id:str = None,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'accounts/' + id + '/ach_relationships/' + ach_id
    r = requests.delete(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def initiate_deposit(self,amount:str = None,id:str = None):

    id = self._set_id(id)
    try:
      ach_id = self.get_ach_data(id)[0]['id']
    except:
      raise Exception('Account ' + id + ' does not have an ACH relationship set up.')

    data =  {'transfer_type': 'ach',
             'relationship_id': ach_id,
             'amount': amount,
             'direction': 'INCOMING'}

    url = self._base_url + 'accounts/' + id + '/transfers'
    r = requests.post(url=url,json=data,auth=self._auth)
    self._check_status(r)
    return r.json()

  def initiate_withdrawal(self,amount:str = None,id:str = None):
    
    id = self._set_id(id)
    try:
      ach_id = self.get_ach_data(id)[0]['id']
    except:
      raise Exception('Account ' + id + ' does not have an ACH relationship set up.')

    data =  {'transfer_type': 'ach',
             'relationship_id': ach_id,
             'amount': amount,
             'direction': 'OUTGOING'}
    
    url = self._base_url + 'accounts/' + id + '/transfers'
    r = requests.post(url=url,json=data,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_transfers(self,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'accounts/' + id + '/transfers'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def delete_transfer(self,transfer_id:str = None,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'accounts/' + id + '/transfers/' + transfer_id
    r = requests.delete(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_documents(self,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'accounts/' + id + '/documents'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def upload_document(self,document_type:str = None,mime_type:str = None,base64:bytes = None,id:str = None):
    id = self._set_id(id)

    data = [{'document_type': document_type,
            'content': base64,
            'mime_type': mime_type}]

    url = self._base_url + 'accounts/' + id + '/documents/upload'
    r = requests.post(url=url,json=data,auth=self._auth)
    self._check_status(r)
    return r.json()

  def download_document(self,document_id:str = None,id:str = None):
    id = self._set_id(id)
    url = self._base_url + 'accounts/' + id + '/documents/' + document_id + '/download'
    r = requests.get(url=url,allow_redirects=True,auth=self._auth)
    self._check_status(r)
    return open('broker-document.pdf', 'wb').write(r.content)

  def get_journals(self):
    url = self._base_url + 'journals'
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def get_journal(self,journal_id:str = None):
    url = self._base_url + 'journals/' + journal_id
    r = requests.get(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def create_journal(self,amount:str = None,to_account:str=None,description:str = None,id:str = None):
    id = self._set_id(id)

    data = {'from_account': id,
            'entry_type': 'JNLC',
            'to_account': to_account,
            'amount': amount,
            'description': description}

    url = self._base_url + 'journals'
    r = requests.post(url=url,json=data,auth=self._auth)
    self._check_status(r)
    return r.json()

  def reverse_batch_journal(self,entries:list = None):

    data = {'entry_type': 'JNLC',
            'to_account': self._master_id,
            'entries': entries}

    url = self._base_url + 'journals'
    r = requests.post(url=url,json=data,auth=self._auth)
    self._check_status(r)
    return r.json()

  def delete_journal(self,journal_id:str = None):
    url = self._base_url + 'journals/' + journal_id
    r = requests.delete(url=url,auth=self._auth)
    self._check_status(r)
    return r.json()

  def close_account(self,id:str = None):
    id = self._set_id(id)

    for i in range(3):
      self.cancel_all_orders(id)
      time.sleep(3)
      if len(self.get_all_orders(id)) > 0: continue
      else: break
    if len(self.get_all_orders(id)) > 0:
      raise Exception('Account still has open orders and cannot be closed.')

    for i in range(3):
      self.close_all_positions(id)
      time.sleep(3)
      if len(self.close_all_positions(id)) > 0: continue
      else: break
    if len(self.get_all_positions(id)) > 0:
      raise Exception('Account still has open positions and cannot be closed.')

    url = self._base_url + 'accounts/' + id
    r = requests.delete(url=url,auth=self._auth)
    self._check_status(r)

    time.sleep(3)

    if self.get_brokerage_account(id)['status'] != 'ACCOUNT_CLOSED':
      raise Exception('Account ' + id + ' failed to close.')

    return print('Account ' + id + ' was successfully closed.')