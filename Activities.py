from gclogic import AlpacaAPI, AzureAPI
from datetime import timedelta as td
from datetime import datetime as dt
from dateutil import parser as ps
from pytz import timezone as tz

alpaca = AlpacaAPI.initiate_API(live=LIVE)
azure = AzureAPI.initiate_API(live=LIVE)
est = tz('US/Eastern')

def handle_trade_activities():

    azure = AzureAPI.initiate_API()
    df = azure.retrieve_table('activities').set_index('event_id')

    yesterday = ps.parse((dt.utcnow() - td(1)).astimezone(est).strftime('%Y-%m-%d')).isoformat(sep=' ', timespec='milliseconds')

    for id in alpaca.get_account_ids(active=True,funded=True):
        alpaca_account_id = alpaca.account_id = id
        orders = alpaca.get_all_orders(status='closed',date=yesterday,id=id)
        for order in orders:
            status = order['status']
            if status == 'filled':

                event_id = str(order['id'])

                if event_id not in df.index.values:
                    values = {
                        'alpaca_account_id':alpaca_account_id,
                        'timestamp':ps.parse(order['filled_at']).isoformat(sep=' ', timespec='milliseconds')[:-6],
                        'symbol':order['symbol'].split('USD')[0],
                        'side':order['side'].capitalize(),
                        'price':float(order['filled_avg_price']),
                        'quantity':float(order['filled_qty']),
                        'order_id':order['client_order_id'],
                        'event_id':event_id
                    }
                    azure.insert_table_row('activities',values)
                    azure.commit_changes()
    azure.close_connection()