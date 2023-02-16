from datetime import timedelta as td
from datetime import datetime as dt
from dateutil import parser as ps
from pytz import timezone as tz
import numpy as np

alpaca = AlpacaAPI.initiate_API(live=LIVE)
azure = AzureAPI.initiate_API(live=LIVE)
est = tz('US/Eastern')

def handle_billing():

  now = dt.utcnow().astimezone(est)
  mth_end = now.replace(day=1) - td(days=1)
  mth_start = now.replace(day=1) - td(days=mth_end.day)
  month = mth_start.strftime('%B')
  year = mth_start.strftime('%Y')
  date = month + ' ' + year
  bill_per = mth_start.strftime('%D') + ' - ' + mth_end.strftime('%D')

  for id in alpaca.get_account_ids(active=True,funded=True):
    if id not in alpaca._master_accounts:
      alpaca_account_id = alpaca.account_id = id
      account = alpaca.get_brokerage_account()
      account_number = account['account_number']
      first_name = account['identity']['given_name']
      last_name = account['identity']['family_name']
      client_name = first_name + ' ' + last_name
      aum = np.round(alpaca.get_prev_mth_avg_equity(),decimals=2)
      fee_pct = 0.00125
      fee_amt = str(np.round(aum * fee_pct,decimals=2))
      description = date + ' Management Fee: ' + "${:,.2f}".format(aum) + ' @ ' + '{:.3%}'.format(fee_pct)
      created_at = dt.utcnow().strftime('%m/%d/%Y')

      values = {'alpaca_account_id':alpaca_account_id,
                'client_name':client_name,
                'account_number':account_number,
                'aum':aum,
                'fee_pct':fee_pct,
                'fee_amt':fee_amt,
                'billing_cycle':bill_per,
                'description':description,
                'created_at':created_at}

      df = azure.retrieve_table('invoiceData')
      redundant = len(df[(df.alpaca_account_id == id) & (df.billing_cycle == bill_per)]) > 0
      #redundant = len([j for j in alpaca.get_journals() if id in j['from_account'] and date in j['description']]) > 0
      no_fee_due = fee_amt == '0.0'
      if redundant:
        print('Account ' + id + ' has already been billed for last month. Another billing journal entry will not be created.')
      elif no_fee_due:
        print('Account ' + id + ' does not owe any fees from the previous month.')
      else:
        try:
          azure.insert_table_row('invoiceData',values)
        except:
          raise Exception('There was an error updating the database. Journal entry will not be created.')

        try:
          journal = alpaca.create_journal(fee_amt,alpaca._sweep_account_id,description,id)
          journal_id = journal['id']
          print('Created billing journal entry for ' + id + '.')
        except:
          azure.cursor.rollback()
          raise Exception('The journal entry could not be created. Rolling back the changes to the database.')

        azure.commit_changes()
        df = azure.retrieve_table('invoiceData').set_index('ID')
        ID = str(df[(df.alpaca_account_id == id) & (df.description == description)].index.values[0])
        azure.update_table_value('invoiceData','journal_id',journal_id,'ID',ID)
        azure.commit_changes()

        user_info = azure.get_user_info(id)
        user_id = user_info.ID
        client_name = user_info.first_name
        client_email = user_info.email

        subject = 'Invoice Available'
        body = 'This email is to inform you that a new invoice has been posted to your account. Login into your account and navigate to the documents section to view it.'

        send_email = azure.retrieve_table_value('userConfigurations','invoice_email','user_id',user_id)
        if send_email:
          hf.send_event_email(client_name,client_email,subject,body,LIVE)

        event_title = 'Invoice Available'
        event_message = 'A new invoice has been posted to the documents section of your account.'
        timestamp = ps.parse(alpaca.get_clock()['timestamp']).isoformat(sep=' ', timespec='milliseconds')[:-6]
        reference_id = journal_id

        values = {
            'alpaca_account_id':alpaca_account_id,
            'reference_id':reference_id,
            'event_type':'invoice',
            'event_title':event_title,
            'event_message':event_message,
            'timestamp':timestamp,
            'status':'UNREAD'
        }
        azure.insert_table_row('notifications',values)
        azure.commit_changes()
  azure.close_connection()