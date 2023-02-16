from datetime import timedelta as td
from datetime import datetime as dt
from pytz import timezone as tz
import pandas as pd
import numpy as np
import random
import time

alpaca = AlpacaAPI.initiate_API(live=LIVE)
azure = AzureAPI.initiate_API(live=LIVE)
est = tz('US/Eastern')
utc = tz('UTC')

env = azure._determine_environment()
if env == 'PythonAnywhere': TEST = False

class RebalanceHandler(object):

    def __init__(self):
      self._account_id = None

    def rebalance_portfolios(self):

        if (alpaca.get_clock()['is_open'] == False) and (TEST == False):
          return print('***Market is not open. Skipping rebalances.***')
        else:
          ids = alpaca.get_account_ids(active=True,funded=True)
          random.shuffle(ids)
          for id in ids:
              print('***Initiating rebalance for ' + id + '.***')
              status = azure.retrieve_table_value('accounts','status','alpaca_account_id',id)
              if status == 'PENDING_CLOSE':
                print('Account',id,'is pending close. Liquidating positions and closing the account.')
                alpaca.close_account(id)
              else:
                self._rebalance_portfolio(id)
              print('***Finished rebalance for ' + id + '.***')
          return print('***Attempt to rebalance all portfolios has been completed succesfully.***')

    def _check_redundancy(self):

      id = self._account_id

      df = azure.retrieve_table('accounts').set_index('alpaca_account_id')

      last_rebalanced = df.loc[id,'last_rebalanced']
      if type(last_rebalanced) == pd.Timestamp: last_rebalanced = pd.Timestamp(last_rebalanced).tz_localize(utc).tz_convert(est).strftime('%Y-%m-%d')
      today = dt.utcnow().astimezone(est).strftime('%Y-%m-%d')
      if last_rebalanced == today: redundant = True
      else: redundant = False

      return redundant

    def _construct_portfolio(self):

      id = self._account_id

      df = azure.retrieve_table('accounts').set_index('alpaca_account_id')

      if id not in df.index.values:
        print('Account ' + id + ' does not have a portfolio_type set. Defaulting to standard portfolio.')
        return Portfolios.standard_3()
      else:
        strategy = df.loc[id,'strategy']

        #CRYPTO
        if strategy == 'cryptocurrency_3':
          self._leverage = .99
          return Portfolios.cryptocurrency_3()

        elif strategy == 'cryptocurrency_2':
          self._leverage = .99
          return Portfolios.cryptocurrency_2()

        elif strategy == 'cryptocurrency_1':
          self._leverage = .99
          return Portfolios.cryptocurrency_1()

        elif strategy == 'cryptocurrency_0':
          self._leverage = .99
          return Portfolios.cryptocurrency_0()

        if alpaca.is_trading_day('month_start'):  

          #INDIVIDUAL STRATS
          if strategy == 'grp':
            self._leverage = .99
            return Portfolios.global_risk_parity(platform='Alpaca')

          elif strategy == 'gda':
            self._leverage = .99
            return Portfolios.global_dynamic_allocation(platform='Alpaca')

          elif strategy == 'gsr':
            self._leverage = .99
            return Portfolios.global_sector_rotation(platform='Alpaca')

          elif strategy == 'gaa':
            self._leverage = .99
            return Portfolios.global_adaptive_alpha(platform='Alpaca')

          elif strategy == 'pb':
            self._leverage = .99
            return Portfolios.portfolio_ballast()

          #MULTI STRATS
          elif strategy == 'agg':
            self._leverage = .99
            return Portfolios.aggressive(platform='Alpaca')

          elif strategy == 'mod':
            self._leverage = .99
            return Portfolios.moderate(platform='Alpaca')

          elif strategy == 'con':
            self._leverage = .99
            return Portfolios.moderate(platform='Alpaca')

          elif strategy == 'dds':
            self._leverage = .99
            return Portfolios.diversified_dynamic_strategies(platform='Alpaca')

        else:
          return 'not_trading_day'

    def _determine_target_cash(self):

      id = self._account_id

      df = azure.retrieve_table('transfers').set_index('ID')
      if len(df[(df.direction == 'OUTGOING') & (df.status == 'PENDING') & (df.withdraw_all == True ) & (df.alpaca_account_id == id)].index.values) > 0:
        target_cash = 'ALL'
      else:
        target_cash = float(df[(df.direction == 'OUTGOING') & (df.status == 'PENDING') & (df.alpaca_account_id == id)].amount.sum())

        if str(alpaca.get_trading_account()['multiplier']) == '1':
          target_cash = target_cash * 1.25
        else:
          target_cash = target_cash * 1.10

      return target_cash

    def _liquidate_stale_positions(self):

      id = self._account_id
      alpaca.cancel_all_orders()
      
      portfolio = self._portfolio.groupby('symbol').sum()
      
      positions = alpaca.get_positions_df().index.values

      if len(positions) > 0:

        for symbol in positions:
          if symbol not in portfolio.index.values:
            if TEST:
              print('Symbol',symbol,'would have been liquidated.')
            else:
              alpaca.close_position(symbol)
              print('***Liquidated',symbol,'position.***')

              for i in range(3):
                if len(alpaca.get_all_orders()) > 0:
                  time.sleep(3)
                  continue
                else: break
              if len(alpaca.get_all_orders()) > 0:
                raise Exception('Account',id,'still has open orders and cannot be rebalanced.')

    def _rebalance_portfolio(self,id:str = None):

      alpaca.account_id = self._account_id = id

      self._is_redundant = self._check_redundancy()
      if self._is_redundant and not TEST:
        print('Skipping rebalance for ' + id + '. Portfolio has already been rebalanced today.')
      else:
        alpaca.patch_account_configs()
        self._leverage = 0.99
        if str(alpaca.get_trading_account()['multiplier']) == '1':
          self._leverage = 0.99

        self._portfolio = self._construct_portfolio()
        if type(self._portfolio) == str:
          print('Skipping rebalance for ' + id + '. This strategy does not trade today.')
        else:
          df = self._portfolio.groupby('symbol').sum()

          self._liquidate_stale_positions()

          leverage = self._leverage
          target_cash = self._determine_target_cash()
          equity = alpaca.get_account_equity()

          if target_cash == 'ALL':
            alpaca.close_all_positions()
            print('Account ' + id + ' has a pending request to withdraw all funds. Positions were closed and rebalance will be skipped.')
            adj_equity = 0.0
          elif target_cash < 0.0:
            raise Exception('Target cash should not be less than 0.0.')
          elif target_cash > 0.0:
            adj_equity = (equity * leverage) - target_cash
            print('Account has a pending withdrawal. Target cash is',target_cash,'and adjusted equity is',adj_equity)
          else:
            adj_equity = equity * leverage

          #Calculating the target value of each asset
          df['targetvalue'] = (df.weight * adj_equity)
          df['targetvalue'] = df.targetvalue.astype('float64')

          #Calculating the current value of holdings
          positions = alpaca.get_positions_df()
          for s in df.index.values:
            if s in positions.index.values:
                df.loc[s,'currentvalue'] = float(positions.loc[s,'market_value'])
            else:
                df.loc[s,'currentvalue'] = 0.0

          #Getting prices for each asset
          for s in df.index.values:
            price = float(alpaca.get_last_quote(s)['ap'])
            if price > 0.0:
              df.loc[s,'price'] = price
            else:
              price = float(alpaca.get_last_trade(s)['p'])
              if price > 0.0:
                df.loc[s,'price'] = price
              else:
                raise Exception('Alpaca get_last_quote and get_last_trade both returned 0 for the price of',s + '.')

          #Calculating how many shares to buy/sell
          for s in df.index.values:
            if alpaca.get_asset_class(s) != 'crypto':
              if alpaca.get_asset(s)['fractionable'] == True:
                df.loc[s,'shares'] = np.round((df.loc[s,'targetvalue'] - df.loc[s,'currentvalue'])/df.loc[s,'price'],decimals=4)
              else:
                df.loc[s,'shares'] = np.round((df.loc[s,'targetvalue'] - df.loc[s,'currentvalue'])/df.loc[s,'price'],decimals=0)
            else:
              qty = (df.loc[s,'targetvalue'] - df.loc[s,'currentvalue'])/df.loc[s,'price']
              min_qty = float(alpaca.get_asset(s)['min_trade_increment'])
              if min_qty < 0.001: min_qty = 0.001
              digits = len(str(min_qty).split('.')[-1])
              df.loc[s,'shares'] = np.round(int(qty/min_qty) * min_qty,decimals=digits)

          df.sort_values('price',ascending=False,inplace=True)

          #Placing Sell Orders
          for s in df.index.values:
            shares = df.loc[s,'shares']
            if shares < 0:
              symbol = s
              qty = str(abs(shares))
              side = 'sell'
              if TEST:
                print('A',side,'order for',qty,'shares of',symbol,'would have been submitted.')
              else:
                alpaca.submit_order(symbol,qty,side)

          for i in range(180):
            if len(alpaca.get_all_orders()) > 0:
              time.sleep(1)
              continue
            else: break
          if len(alpaca.get_all_orders()) > 0:
            raise Exception('Account ' + id + ' still has open sell orders and buy orders cannot be placed.' + str(alpaca.get_all_orders()))

          #Placing Buy Orders
          for s in df.index.values:
            shares = df.loc[s,'shares']
            if shares > 0:
              symbol = s
              qty = str(abs(shares))
              side = 'buy'
              if TEST:
                print('A',side,'order for',qty,'shares of',symbol,'would have been submitted.')
              else:
                alpaca.submit_order(symbol,qty,side)

          last_rebalanced = azure.retrieve_table('accounts').set_index('alpaca_account_id').loc[id].last_rebalanced
          if type(last_rebalanced) == type(pd.NaT):
            user_info = azure.get_user_info(id)
            client_name = user_info.first_name
            client_email = user_info.email
            subject = 'Funds Invested'
            body = 'Congratulations! Your funds have been invested. Log into your account to see details about your portfolio.'
            hf.send_event_email(client_name,client_email,subject,body,LIVE)

          last_rebalanced = dt.utcnow().isoformat(sep=' ', timespec='milliseconds')
          azure.update_table_value('accounts','last_rebalanced',last_rebalanced,'alpaca_account_id',id)
          if not TEST:
            azure.commit_changes()

        return print('Rebalance for ' + self._account_id + ' was completed successfully.')