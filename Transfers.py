from datetime import datetime as dt
from pytz import timezone as tz
import pandas as pd

alpaca = AlpacaAPI.initiate_API(live=LIVE)
azure = AzureAPI.initiate_API(live=LIVE)
est = tz('US/Eastern')
utc = tz('UTC')

def handle_recurring_deposits():
  df = azure.retrieve_table('recurringDeposits')
  df.set_index('ID',inplace=True)
  df = df[df.status == 'ACTIVE']

  for ID in df.index.values:

    id = alpaca.account_id = df.loc[ID,'alpaca_account_id']
    print('***Handling recurring deposit for account ' + id + '.***')

    freq = df.loc[ID,'frequency']
    day = df.loc[ID,'day']
    amt = str(df.loc[ID,'amount'])

    if freq == 'Daily': today = day
    elif freq == 'Weekly': today = dt.utcnow().astimezone(est).weekday()
    elif freq == 'Monthly': today = dt.utcnow().astimezone(est).day
    else: raise Exception('Got an unexpected value for frequency property.')

    if day != today:
      print('A recurring deposit is not scheduled for today.')
    else:
      print('A recurring deposit is scheduled for today. Checking for redundancy.')
      
      #Checking if a recurring deposit has already occurred today to avoid redundancy.
      lastInstance = df.loc[ID,'lastInstance']
      if type(lastInstance) == pd.Timestamp: lastInstance = lastInstance.tz_localize(utc).tz_convert(est).strftime('%Y-%m-%d')
      else: lastInstance = None

      redundant = lastInstance == dt.utcnow().astimezone(est).strftime('%Y-%m-%d')

      if redundant:
        print('Recurring deposit has already been initiated today. Another deposit will not be initiated.')
      else:
        print('A recurring deposit has not already been initiated today. Attempting to initiate deposit.')
        
        try:
          lastInstance = dt.utcnow().isoformat(sep=' ', timespec='milliseconds')
          azure.update_table_value('recurringDeposits','lastInstance',lastInstance,'ID',ID)
        except:
          raise Exception('There was an error updating the database. Transfer will not be created.')

        try:
          t = alpaca.initiate_deposit(amt)
          print('A recurring deposit of ' + amt + ' was initiated for account ' + id)
          azure.commit_changes()

          transfer_id = t['id']
          direction = str(t['direction']).upper()
          relationship_id = str(t['relationship_id'])
          amount = str(t['amount'])
          requestedDateTime = dt.utcnow().isoformat(sep=' ', timespec='milliseconds')
          status = 'PENDING'
          frequency_type = 'recurring'

          values = {'alpaca_account_id':id,
                    'direction':direction,
                    'relationship_id':relationship_id,
                    'amount':amount,
                    'requestedDateTime':requestedDateTime,
                    'status':status,
                    'transfer_id':transfer_id,
                    'frequency_type':frequency_type}

          azure.insert_table_row('transfers',values)
          print('Inserted new row into transfers table with details for ' + transfer_id + '.')

        except:
          azure.cursor.rollback()
          raise Exception('The transfer could not be created. Rolling back the changes to the database.')

        azure.commit_changes()

    print('***Finsihed handling recurring deposit for account ' + id + '.***')
    print('')

def handle_new_withdrawals():

  print('***Handling new withdrawals.***')

  transfers = azure.retrieve_table('transfers')
  df = transfers[(transfers.transfer_id.isnull()) &
                 (transfers.direction == 'OUTGOING') &
                 (transfers.withdraw_all != True) &
                 (transfers.status != 'CANCELED')].set_index('ID').sort_values('requestedDateTime')

  for ID in df.index.values:
    alpaca.account_id = id = str(df.loc[ID,'alpaca_account_id'])
    amount = str(df.loc[ID,'amount'])
    amt = float(amount)
    cash_withdrawable = float(alpaca.get_trading_account()['cash_withdrawable'])
    equity = float(alpaca.get_account_equity())
    if amt > equity:
      azure.update_table_value('transfers','status','CANCELED','ID',ID)
      azure.commit_changes()
      print('Transfer ' + str(ID) + ' for account ' + id + ' was canceled because amount is greater than equity.')
    elif 'SUBMITTED' in transfers[transfers.alpaca_account_id == id].status.values:
        print('Account ' + id + ' already has a pending withdrawal and a new one cannot be submitted.')
    elif amt > cash_withdrawable:
      print('Account ' + id + ' does not have enough withdrawable cash to initiate pending withdrawal transfer ' + str(ID) + '.')
    else:
      transfer = alpaca.initiate_withdrawal(amount)
      transfer_id = str(transfer['id'])
      azure.update_table_value('transfers','transfer_id',transfer_id,'ID',ID)
      print('Transfer ' + transfer_id + ' was initiated successfuly for account '+ id + '.')
      azure.commit_changes()

  print('***Finished handling new withdrawals.***')

def handle_withdraw_alls():

  print('***Handling withdraw alls.***')

  transfers = azure.retrieve_table('transfers')
  df = transfers[(transfers.transfer_id.isnull()) &
                 (transfers.direction == 'OUTGOING') &
                 (transfers.withdraw_all == True) &
                 (transfers.status == 'PENDING')].set_index('ID').sort_values('requestedDateTime')

  for ID in df.index.values:
    alpaca.account_id = id = str(df.loc[ID,'alpaca_account_id'])
    equity = float(alpaca.get_account_equity())
    cash_withdrawable = float(alpaca.get_trading_account()['cash_withdrawable'])
    if equity + cash_withdrawable == 0.00:
      print('Cancelling withdraw all request for account' + str(id) + ' as they have no funds.')
      azure.update_table_value('transfers','status','CANCELED','ID',ID)
      azure.commit_changes()
    elif equity != cash_withdrawable:
      print('Account ' + id + ' does not have enough withdrawable cash to initiate pending withdrawal all transfer ' + str(ID) + '.')
    else:
      amt = round(cash_withdrawable,ndigits=1)
      transfer = alpaca.initiate_withdrawal(amt)
      transfer_id = str(transfer['id'])
      azure.update_table_value('transfers','transfer_id',transfer_id,'ID',ID)
      azure.update_table_value('transfers','amount',float(amt),'ID',ID)
      print('Transfer ' + transfer_id + ' was initiated successfuly for account '+ id + '.')
      azure.commit_changes()

  print('***Finished handling withdraw alls.***')