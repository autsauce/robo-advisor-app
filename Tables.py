from gclogic import AlpacaAPI, AzureAPI
from datetime import timedelta as td
from datetime import datetime as dt
from dateutil import parser as ps
import pandas as pd
import quandl

alpaca = AlpacaAPI.initiate_API(live=LIVE)
azure = AzureAPI.initiate_API(live=LIVE)

def update_accounts_table():

    print('***Updating accounts table.***')

    financial = azure.retrieve_table('financialInfo').set_index('user_id')
    investment = azure.retrieve_table('investmentInfo').set_index('alpaca_account_id')
    accounts = azure.retrieve_table('accounts').set_index('alpaca_account_id')
    df = accounts[accounts.posted_to_alpaca == True]

    for id in df.index.values:
      user_id = df.loc[id,'user_id']
      current_strategy = df.loc[id,'strategy']

      portfolio_type = investment.loc[id,'portfolio_type']

      objective = investment.loc[id,'objective'].lower()
      tolerance = investment.loc[id,'tolerance'].lower()
      horizon = investment.loc[id,'time_horizon'].lower()

      risk_map = {
            'aggressive':3,
            'moderate':2,
            'conservative':1,
            'speculation':3,
            'growth':2,
            'income':1,
            'long':3,
            'medium':2,
            'short':1
            }

      risk_metric = risk_map[objective] + risk_map[tolerance] + risk_map[horizon]
      if float(alpaca.get_account_equity(id)) < 500.00: risk = '_0'
      elif risk_metric in [3,4]: risk = '_1'
      elif risk_metric in [5,6]: risk = '_2'
      elif risk_metric in [7,8,9]: risk = '_3'
      else: raise Exception('Got an unexpected value for risk_metric.')

      if LIVE:
        strategy = 'cryptocurrency' + risk
      elif portfolio_type in ['recommend', None]:
        strategy = 'cryptocurrency' + risk
      elif portfolio_type in ['savings','alpha','grp','gda','gsr','gaa','dds','pb','aggressive','moderate','conservative']:
        strategy = portfolio_type
      else:
        strategy = 'gaa'

      if current_strategy != strategy:
        azure.update_table_value('accounts','strategy',strategy,'alpaca_account_id',id)
        azure.commit_changes()

    print('***Finished updating accounts table.***')

def update_transfers_table():

  print('***Updating transfers table.***')

  ids = alpaca.get_account_ids()
  df = azure.retrieve_table('transfers').set_index('ID')

  for id in ids:
    alpaca.account_id = id
    transfers = alpaca.get_transfers()
    for t in transfers:
      transfer_id = str(t['id'])
      direction = str(t['direction']).upper()
      relationship_id = str(t['relationship_id'])
      amount = str(t['amount'])
      requestedDateTime = ps.parse(t['created_at']).isoformat(sep=' ', timespec='milliseconds')[:-6]
      status = str(t['status']).upper()
      if status == 'CANCELED': status = status
      elif status in ['REJECTED','RETURNED']: status = 'REJECTED'
      elif status == 'COMPLETE': status = 'COMPLETED'
      elif status == 'QUEUED': status = 'PENDING'
      else: status = 'SUBMITTED'
      frequency_type = 'unknown'

      values = {'alpaca_account_id':id,
                'direction':direction,
                'relationship_id':relationship_id,
                'amount':amount,
                'requestedDateTime':requestedDateTime,
                'status':status,
                'transfer_id':transfer_id,
                'frequency_type':frequency_type}

      #Adding any transfers that may have somehow been missed into transfers table from Alpaca.
      if transfer_id not in df.transfer_id.values:
        print('Adding transfer ' + transfer_id + ' to transfers table.')
        azure.insert_table_row('transfers',values)
        azure.commit_changes()
        print('Inserted new row into transfers table with details for ' + transfer_id + '.')
      #Updating any statuses that did not get updated by transfer events handler.
      else:
        current_status = azure.retrieve_table_value('transfers','status','transfer_id',transfer_id)
        if current_status != status:
          azure.update_table_value('transfers','status',status,'transfer_id',transfer_id)
          azure.commit_changes()

  print('***Finished updating transfers table.***')

def update_historicalData_table():

  quandl.ApiConfig.api_key = "tZowwzzJLTGsuZBrTAyq"
  quandl.export_table('SHARADAR/TICKERS',table='SF1')
  metadata = pd.read_csv('SHARADAR_TICKERS.zip')

  last_entry = azure.retrieve_table('historicalData').timestamp.max().strftime('%Y-%m-%d')
  last_price_date = metadata.lastpricedate.dropna().max()

  if last_entry != last_price_date:

    print('***Updating historicalData table.***')

    #Developed Countries
    dc = ['Canada','Austria','Belgium','Denmark','Finland','France','Germany','Ireland','Israel',
          'Italy','Netherlands','Norway','Portugal','Spain','Sweden','Switzerland','United Kingdom',
          'Australia','Japan','New Zealand','Singapore']

    #Emerging Countries
    ec = ['Argentina','Brazil','Chile','Columbia','Mexico','Peru','Egypt','Greece','Hungary',
          'Poland','Russa','Turkey','China','India','Korea','Pakistan','Taiwan','Thailand','Hong Kong']


    dom_tickers = list(metadata[(metadata.category == 'Domestic Common Stock') &
                                (metadata.isdelisted == 'N') &
                                (metadata.exchange != 'OTC')].ticker.unique())

    dev_tickers = list(metadata[(metadata.category == 'ADR Common Stock') &
                                (metadata.location.isin(dc)) &
                                (metadata.isdelisted == 'N') &
                                (metadata.exchange != 'OTC')].ticker.unique())

    emg_tickers = list(metadata[(metadata.category == 'ADR Common Stock') &
                                (metadata.location.isin(ec)) & 
                                (metadata.isdelisted == 'N') & 
                                (metadata.exchange != 'OTC')].ticker.unique())


    today = pd.Timestamp(dt.utcnow())
    past = (today - pd.Timedelta(365,'D')).strftime('%Y-%m-%d')

    quandl.export_table('SHARADAR/SEP',date={'gte':past})
    prices = pd.read_csv('SHARADAR_SEP.zip')

    prices = prices[prices.ticker.isin(prices.ticker.value_counts()[prices.ticker.value_counts() == prices.ticker.value_counts().max()].index)]
    prices = prices.sort_values(['ticker','date'])
    prices['adv'] = (prices.close * prices.volume).rolling(63).mean()
    max_date = prices.date.max()

    dom_data = prices[prices.ticker.isin(dom_tickers)]
    dev_data = prices[prices.ticker.isin(dev_tickers)]
    emg_data = prices[prices.ticker.isin(emg_tickers)]

    timestamp = ps.parse(max_date).isoformat(sep=' ', timespec='milliseconds')
    dom_ticker = dom_data[dom_data.date == max_date].groupby('ticker').max().adv.idxmax()
    dev_ticker = dev_data[dev_data.date == max_date].groupby('ticker').max().adv.idxmax()
    emg_ticker = emg_data[emg_data.date == max_date].groupby('ticker').max().adv.idxmax()

    values = {'timestamp':timestamp,
              'dom_ticker':dom_ticker,
              'dev_ticker':dev_ticker,
              'emg_ticker':emg_ticker}

    azure.insert_table_row('historicalData',values)
    azure.commit_changes()

    print('***Finished updating historicalData table.***')

def update_assetMap_table():

    print('***Updating assetMap table.***')

    df = pd.read_csv('Asset Map.csv').set_index('symbol')
    assetMap = azure.retrieve_table('assetMap').set_index('symbol')

    for i in df.index.values:
        if i not in assetMap.index.values:
            print('Inserting new row for',i)
            values = {
                'symbol':i,
                'icon':df.loc[i,'icon'],
                'name':df.loc[i,'name'],
                'class':df.loc[i,'class'],
                'category':df.loc[i,'category'],
                'volatility':df.loc[i,'volatility'],
                'harvest':df.loc[i,'harvest'],
                'tilt':df.loc[i,'tilt'],
                'internal_class':df.loc[i,'internal_class'],
                'internal_category':df.loc[i,'internal_category'],
            }
            azure.insert_table_row('assetMap',values)
            azure.commit_changes()
        else:
            cols = df.columns.values
            for c in cols:
                old = assetMap.loc[i,c]
                new = str(df.loc[i,c])
                if old != new:
                    print('Updating old values for',i)
                    ID = assetMap.loc[i,'ID']
                    azure.update_table_value('assetMap',c,new,'ID',ID)
                    azure.commit_changes()
                else:
                    pass

    print('***Finished updating assetMap table.***')