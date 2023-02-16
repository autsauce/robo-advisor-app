from datetime import timedelta as td
from datetime import datetime as dt
from dateutil import parser as ps
from pytz import timezone as tz
import requests
import json

alpaca = AlpacaAPI.initiate_API(live=LIVE)
azure = AzureAPI.initiate_API(live=LIVE)
est = tz('US/Eastern')

def handle_account_notifications():

    yesterday = (dt.utcnow() - td(1)).astimezone(est).strftime('%Y-%m-%d')
    params = {'since':yesterday}
    url = alpaca._base_url + 'events/accounts/status'

    with requests.get(url, auth=alpaca._auth, params=params, stream=True) as response:
        lines = response.iter_lines(decode_unicode=True)
        for line in lines:
            if ('data:' in line) & ('crypto' not in line):
                data = json.loads(line[5:])
                print(data)
                alpaca_account_id = alpaca.account_id = id = data['account_id']

                if id not in alpaca._master_accounts:
                    azure = AzureAPI.initiate_API(live=LIVE)
                    print('Live environment is equal to',LIVE)

                    notifications = azure.retrieve_table('notifications')
                    df = notifications[notifications.event_type == 'account'].set_index('reference_id')

                    reference_id = str(data['event_id'])
                    if reference_id in df.index.values:
                        print('Notification already exists for event ID ' + reference_id + '.')
                    else:
                        status = data['status_to']

                        if status in ['ACTIVE','REJECTED','DISABLED','ACTION_REQUIRED']: status = status
                        elif status == 'ACCOUNT_CLOSED': status = 'CLOSED'
                        else: status = 'PENDING'

                        azure = AzureAPI.initiate_API(live=LIVE)
                        print('Live environment is equal to',LIVE)
                        azure.update_table_value('accounts','status',status,'alpaca_account_id',id)
                        azure.commit_changes()

                        if status != 'PENDING':
                            if status == 'ACTIVE':
                                subject = 'Account Approved'
                                body = '''
                                        Your account has been approved! The next step is to fund your account.
                                        <br>
                                        <br>
                                        If you wish to transfer funds from an existing investment account, use the following link to get started:
                                        <br>
                                        <br>
                                        <b><u><a target="_blank" href="X">Request an Account Transfer</a></u></b>
                                        <br>
                                        <br>
                                        You may also transfer funds directly from your bank account by navigating to the "Banking" section of your account.
                                    '''  
                            elif status == 'REJECTED':
                                subject = 'Account Rejected'
                                body = 'Your account has been rejected. We will be in touch with more details regarding your account application.'
                            elif status == 'DISABLED':
                                subject = 'Account Disabled'
                                body = 'Your account has been disabled. We will be in touch with more details regarding your account status.'
                            elif status == 'ACTION_REQUIRED':
                                subject = 'Action Required'
                                body = 'Your account requires additional action. We will be in touch with more details regarding your account application.'
                            elif status == 'CLOSED':
                                subject = 'Account Closed'
                                body = 'Your account has been closed at your request. Any of your remaining positions have been liquidated and your funds should be ready to withdraw in the next couple of business days.'
                            else:
                                raise Exception('Unexpected value for status.')

                            user_info = azure.get_user_info(id)
                            client_name = user_info.first_name
                            client_email = user_info.email
                            hf.send_event_email(client_name,client_email,subject,body,LIVE)

                            event_title = subject
                            event_message = subject
                            timestamp = ps.parse(alpaca.get_clock()['timestamp']).isoformat(sep=' ', timespec='milliseconds')[:-6]

                            values = {
                                'alpaca_account_id':alpaca_account_id,
                                'reference_id':reference_id,
                                'event_type':'account',
                                'event_title':event_title,
                                'event_message':event_message,
                                'timestamp':timestamp,
                                'status':'UNREAD',
                            }

                            azure.insert_table_row('notifications',values)
                            print('Added event ID ' + reference_id + ' to Notifications table.')
                            azure.commit_changes()
                            azure.close_connection()

def handle_transfer_notifications():

    yesterday = (dt.utcnow() - td(1)).astimezone(est).strftime('%Y-%m-%d')
    params = {'since':yesterday}
    url = alpaca._base_url + 'events/transfers/status'

    with requests.get(url, auth=alpaca._auth, params=params, stream=True) as response:
        lines = response.iter_lines(decode_unicode=True)
        for line in lines:
            if ('data:' in line):
                data = json.loads(line[5:])
                print(data)
                alpaca_account_id = alpaca.account_id = id = data['account_id']

                if id not in alpaca._master_accounts:
                    azure = AzureAPI.initiate_API(live=LIVE)

                    notifications = azure.retrieve_table('notifications')
                    notifications = notifications[notifications.event_type == 'transfer'].set_index('reference_id')

                    df = azure.retrieve_table('transfers').set_index('transfer_id')

                    reference_id = str(data['event_id'])
                    if reference_id in notifications.index.values:
                        print('Notification already exists for event ID ' + reference_id + '.')
                    else:
                        
                        transfer_id = data['transfer_id']
                        status = data['status_to']
                        if status in ['REJECTED','RETURNED']: status = 'REJECTED'
                        elif status == 'CANCELED': status = status
                        elif status == 'COMPLETE': status = 'COMPLETED'
                        elif status == 'QUEUED': status = 'PENDING'
                        else: status = 'SUBMITTED'

                        azure = AzureAPI.initiate_API(live=LIVE)
                        azure.update_table_value('transfers','status',status,'transfer_id',transfer_id)
                        azure.commit_changes()

                        if transfer_id in df.index.values:
                            if status in ['COMPLETED','REJECTED']:
                                amount = df.loc[transfer_id,'amount']
                                direction = str(df.loc[transfer_id,'direction'])
                                requestedDateTime = df.loc[transfer_id,'requestedDateTime']
                                transfer_freq = str(df.loc[transfer_id,'frequency_type']).replace('_','-')
                                transfer_date = requestedDateTime.strftime('%B %d, %Y')
                                transfer_time = requestedDateTime.strftime('%H:%M:%S')
                                transfer_state = status.lower()
                                transfer_amount = "${:,.2f}".format(amount)

                                if direction == 'INCOMING': transfer_type = 'deposit' 
                                elif direction == 'OUTGOING': transfer_type = 'withdrawal'
                                else: raise Exception('Unexpected value for direction.')

                                user_info = azure.get_user_info(id)
                                user_id = user_info.ID
                                client_name = user_info.first_name
                                client_email = user_info.email
                                subject = transfer_type.capitalize() + ' ' + transfer_state.capitalize()
                                body = '''This email is to inform you that your {transfer_freq} {transfer_type} for {transfer_amount} submitted on
                                        {transfer_date} was {transfer_state}.'''.format(transfer_freq=transfer_freq,
                                                                                        transfer_type=transfer_type,
                                                                                        transfer_amount=transfer_amount,
                                                                                        transfer_date=transfer_date,
                                                                                        transfer_state=transfer_state)
                                
                                send_email = azure.retrieve_table_value('userConfigurations','bank_transfer_email','user_id',user_id)
                                if send_email:
                                    hf.send_event_email(client_name,client_email,subject,body,LIVE)

                                event_message = 'Your {transfer_type} for {transfer_amount} was {transfer_state}.'.format(transfer_type=transfer_type,
                                                                                                                                transfer_amount=transfer_amount,
                                                                                                                                transfer_state=transfer_state)

                                timestamp = ps.parse(alpaca.get_clock()['timestamp']).isoformat(sep=' ', timespec='milliseconds')[:-6]

                                values = {
                                        'alpaca_account_id':alpaca.account_id,
                                        'reference_id':reference_id,
                                        'event_type':'transfer',
                                        'event_title':subject,
                                        'event_message':event_message,
                                        'timestamp':timestamp,
                                        'status':'UNREAD',
                                        }

                                azure.insert_table_row('notifications',values)
                                print('Added event ID ' + reference_id + ' to Notifications table.')
                                azure.commit_changes()
                                azure.close_connection()

def handle_document_notifications():

    print('***Handling document notifications.***')

    notifications = azure.retrieve_table('notifications').set_index('ID')
    df = notifications[notifications.event_type == 'document']

    ids = alpaca.get_account_ids(active=True,funded=True)
    for id in ids:
        alpaca.account_id = id
        documents = alpaca.get_documents()
        for d in documents:
            document_type = d['type'].replace('_',' ')
            if 'json' not in document_type:
                reference_id = d['id']
                if reference_id not in df.reference_id.values:

                    user_info = azure.get_user_info(id)
                    user_id = user_info.ID
                    client_name = user_info.first_name
                    client_email = user_info.email

                    subject = document_type.title() + ' Available'
                    body = 'This email is to inform you that a new {document_type} has been posted to your account. Login into your account and navigate to the documents section to view it.'.format(document_type=document_type)

                    if document_type == 'tax statement':
                        send_email = True
                    else:
                        column_name = (document_type + ' email').replace(' ','_')
                        send_email = azure.retrieve_table_value('userConfigurations',column_name,'user_id',user_id)
                    if send_email:
                        hf.send_event_email(client_name,client_email,subject,body,LIVE)
                    
                    event_title = subject
                    event_message = 'A new {document_type} has been posted to the documents section of your account.'.format(document_type=document_type)
                    timestamp = ps.parse(alpaca.get_clock()['timestamp']).isoformat(sep=' ', timespec='milliseconds')[:-6]

                    values = {
                        'alpaca_account_id':alpaca.account_id,
                        'reference_id':reference_id,
                        'event_type':'document',
                        'event_title':event_title,
                        'event_message':event_message,
                        'timestamp':timestamp,
                        'status':'UNREAD'
                    }
                    azure.insert_table_row('notifications',values)
                    print('Added document ID ' + reference_id + ' to Notifications table.')
                    azure.commit_changes()

    azure.close_connection()

    print('***Finished handling document notifications.***')