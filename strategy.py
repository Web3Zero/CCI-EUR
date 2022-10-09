import os
import json
import toml
import ib_insync
import pandas as pd
import pandas_ta as ta
from loguru import logger
from threading import Timer
from datetime import datetime
from traceback import format_exc


class Position:
    def __init__(self):
        self.side = ''
        self.entry_timestamp = None
        self.entry_price = None
        self.entry_cci = None
        self.size = None
        self.exit_timestamp = None
        self.exit_price = None
        self.exit_cci = None
        self.pnl = None
    
    def __bool__(self):
        if not self.entry_timestamp:
            return False
        elif self.exit_timestamp and self.exit_timestamp:
            return False
        else:
            return True
    
    def __str__(self):
        return json.dumps(self.to_dict(), indent=2, default=str)
    
    def open(self, price, side, cci, size):
        assert bool(size)
        assert bool(price)
        assert side in ('long', 'short')
        self.side = side
        self.size = size
        self.entry_cci = cci
        self.entry_price = price
        self.entry_timestamp = datetime.utcnow()

    def close(self, price, cci):
        assert bool(price)
        self.exit_cci = cci
        self.exit_price = price
        self.exit_timestamp = datetime.utcnow()
        price_change = ((self.exit_price - self.entry_price) / self.entry_price) * 100
        self.pnl = price_change if self.side == 'long' else - price_change
    
    def is_open(self):
        return self.__bool__()
    
    def to_dict(self):
        return {
            'entry_timestamp': self.entry_timestamp,
            'entry_price': self.entry_price,
            'entry_cci': self.entry_cci,
            'size': self.size,
            'exit_timestamp': self.exit_timestamp,
            'exit_price': self.exit_price,
            'exit_cci': self.exit_cci,
            'pnl': self.pnl
        }


class Strategy:
    def __init__(self, ib_host: str, ib_port: int, asset_symbol: str, asset_security_type: str, asset_exchange: str, asset_currency: str, asset_data_type: str,
                 timeframe: str, cci_length: int, save_data_to_csv: str, save_data_to_csv_interval: int, order_size: int, order_send_interval: int,
                 cci_long_entry_threshold: float, cci_long_exit_threshold: float, cci_short_entry_threshold: float, cci_short_exit_threshold: float):
        '''
        CCI strategy using 5 sec live bars
        Ref: 
            https://interactivebrokers.github.io/tws-api/historical_bars.html
            https://interactivebrokers.github.io/tws-api/realtime_bars.html
        Args:
            ib_host: The host address of the TWS Workstation or IB gateway
            ib_port: The port number of the IB connection
            asset_symbol: Symbol of the asset to trade, must be valid local symbol for the exchange
            asset_security_type: Security type of the asset, e.g. FUT, STK, CASH...
            asset_exchange: Exchange where the asset is traded
            asset_currency: Currency against which asset is traded, most likely USD
            asset_data_type: MIDPOINT or TRADES
            timeframe: 5 sec bars will be resampled to that timeframe, must be valid IB timeframe (see ref link)
            cci_length: Length of the CCI indicator
            save_data_to_csv: CSV file path to save bars data and calculated CCI, leave empty to disable saving
            save_data_to_csv_interval: Interval to save data to CSV in seconds
            order_size: Size of the order that will be placed
            order_send_interval: Data stream tread parallel to order placing thread, this is the order placing thread interval
            cci_long_entry_threshold: If CCI is above that value and there is no open position long position will be opened
            cci_long_exit_threshold: If CCI is below that value and there is open position it will be closed
            cci_short_entry_threshold: If CCI is below that value and there is no open position short position will be opened
            cci_short_exit_threshold: If CCI is above that value and there is open position it will be closed
        '''
        
        self.ib = ib_insync.IB()
        self.ib.connect(ib_host, ib_port, clientId=3, timeout=0)
        self.contract = ib_insync.Contract(
            secType=asset_security_type,
            localSymbol=asset_symbol,
            exchange=asset_exchange,
            currency=asset_currency
        )
        self.asset_data_type = asset_data_type
        assert any(self.ib.qualifyContracts(self.contract))  # Make sure the contract is valid
        
        self.df = pd.DataFrame()
        self.timeframe = timeframe
        self.order_size = order_size
        self.cci_length = cci_length
        self.save_data_to_csv = save_data_to_csv
        self.order_send_interval = order_send_interval
        self.save_data_to_csv_interval = save_data_to_csv_interval

        self.cci_long_entry_threshold = cci_long_entry_threshold
        self.cci_long_exit_threshold = cci_long_exit_threshold
        self.cci_short_entry_threshold = cci_short_entry_threshold
        self.cci_short_exit_threshold = cci_short_exit_threshold
        
        self.positions = [Position()]
        self.pending_order = None
        
        self.wait_for_order_fill = 60  # Seconds to wait for market order to be filled

    def get_historical_bars(self, duration, what_to_show, use_rth, timeout):
        if self.timeframe.endswith('h'):
            timeframe_size = self.timeframe.rstrip('h')
            if int(timeframe_size) > 1:
                bar_size = f'{timeframe_size} hours'
            else:
                bar_size = f'{timeframe_size} hour'
        elif self.timeframe.endswith('min'):
            timeframe_size = self.timeframe.rstrip('min')
            if int(timeframe_size) > 1:
                bar_size = f'{timeframe_size} mins'
            else:
                bar_size = f'{timeframe_size} min'
        data = self.ib.reqHistoricalData(
            self.contract,
            '',
            barSizeSetting=bar_size,
            durationStr=duration,
            whatToShow=what_to_show,
            useRTH=use_rth,
            timeout=timeout,
            formatDate=2
        )
        df = pd.DataFrame([{'timestamp': c.date, 'open': c.open, 'high': c.high, 'low': c.low, 'close': c.close, 'volume': c.volume} for c in data])
        return df.set_index('timestamp')
    
    def update_csv(self):
        file_exists = os.path.isfile(self.save_data_to_csv)
        logger.info(f'{"Updating" if file_exists else "Creating"} {self.save_data_to_csv}')
        
        if file_exists:
            df = pd.read_csv(self.save_data_to_csv, parse_dates=['timestamp']).set_index('timestamp')
            if df.index[-1] == self.df.index[-1]:
                df.iloc[-1] = self.df.iloc[-1]
            else:
                df = pd.concat([df, self.df.tail(1)])
        else:
            df = self.df
        df.to_csv(self.save_data_to_csv)
        self.update_csv_timer_thread = Timer(self.save_data_to_csv_interval, self.update_csv)
        self.update_csv_timer_thread.start()
    
    def on_bar_update(self, bars, has_new_bars):
        if has_new_bars:
            df = ib_insync.util.df(bars, labels=('time', 'open_', 'high', 'low', 'close', 'volume'))
            df = df.rename(columns={'open_': 'open', 'time': 'timestamp'}).set_index('timestamp')
            if self.timeframe:
                df = df.resample(self.timeframe, offset='0s').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                })
            df = pd.concat([self.df, df])
            df = df[~df.index.duplicated(keep='last')]
            df['cci'] = ta.cci(df['high'], df['low'], df['close'], length=self.cci_length)
            df['cci'] = df['cci'].ffill()  # If data is not changing CCI value will be NaN
            logger.debug(f'Last bar: {df.iloc[-1].to_dict()}')  # TODO: Remove to improve speed.
            self.df = df
    
    def trade(self):
        if 'cci' not in self.df:
            return
        
        cci = self.df['cci'].iloc[-1]

        # Close position
        if self.positions[-1]:
            # Long
            if self.positions[-1].side == 'long' and cci < self.cci_long_exit_threshold:
                logger.info(f'Closing long position at cci: {cci}')
                fill_price = self.create_order('short')
                if fill_price:
                    self.positions[-1].close(price=fill_price, cci=cci)
                    logger.debug(f'Position:\n{self.positions[-1]}')
                    self.positions.append(Position())
            # Short
            elif self.positions[-1].side == 'short' and cci > self.cci_short_exit_threshold:
                logger.info(f'Closing short position at cci: {cci}')
                fill_price = self.create_order('long')
                if fill_price:
                    self.positions[-1].close(price=fill_price, cci=cci)
                    logger.debug(f'Position:\n{self.positions[-1]}')
                    self.positions.append(Position())
        # Open position
        else:
            # Long
            if cci > self.cci_long_entry_threshold:
                logger.info(f'Opening long position at cci: {cci}')
                fill_price = self.create_order('long')
                if fill_price:
                    self.positions[-1].open(price=fill_price, side='long', cci=cci, size=self.order_size)
                    logger.debug(f'Position:\n{self.positions[-1]}')
            # Short
            elif cci < self.cci_short_entry_threshold:
                logger.info(f'Opening short position at cci: {cci}')
                fill_price = self.create_order('short')
                if fill_price:
                    self.positions[-1].open(price=fill_price, side='short', cci=cci, size=self.order_size)
                    logger.debug(f'Position:\n{self.positions[-1]}')
    
    def create_order(self, side):
        action = {'long': 'BUY', 'short': 'SELL'}[side]
        order = ib_insync.order.MarketOrder(
            action=action,
            totalQuantity=self.order_size,
            outsideRth=True,
            tif='GTC'
        )
        trade = self.ib.placeOrder(self.contract, order)
        self.ib.sleep(1)
        
        # Wait for order to be filled
        order_fill_price = None
        for _ in range(self.wait_for_order_fill):
            order_status = trade.orderStatus.status
            if order_status in ('Cancelled', 'ApiCancelled', 'Inactive', 'PendingCancel'):
                logger.warning(f'Order was rejected with status {order_status}')
                return
            elif order_status == 'Filled':
                order_fills = [f.execution.avgPrice for f in trade.fills]
                order_fill_price = sum(order_fills) / len(order_fills)
                break
            self.ib.sleep(1)
        
        # If order was not filled try to cancel it
        if not order_fill_price:
            logger.info('Open order was not filled. Cancelling')
            self.ib.cancelOrder(order)
            return
        
        logger.info(f'Order filled at {order_fill_price}')

        return order_fill_price

    def run(self):
        logger.info('Starting')
        
        # Get historical bars
        logger.info('Getting historical bars')
        self.df = self.get_historical_bars(duration='2 D', what_to_show=self.asset_data_type, use_rth=False, timeout=10)
        
        # Subscribe to bars stream
        bars = self.ib.reqRealTimeBars(self.contract, 5, self.asset_data_type, False)
        bars.updateEvent += self.on_bar_update
    
        # Start CSV updating thread
        logger.info('Starting live bars stream')
        if self.save_data_to_csv:
            self.update_csv_timer_thread = Timer(self.save_data_to_csv_interval, self.update_csv)
            self.update_csv_timer_thread.start()

        # Loop until interrupted
        while True:
            try:
                self.trade()
                self.ib.sleep(self.order_send_interval)
            except KeyboardInterrupt:
                logger.info('KeyboardInterrupt')
                break
            except:
                logger.error(format_exc())
                self.ib.sleep(1)
        # Cancel bars stream
        self.ib.cancelHistoricalData(bars)
        # Cancel CSV update
        if self.save_data_to_csv:
            self.update_csv_timer_thread.cancel()


if __name__ == '__main__':
    config = toml.load('config.toml')
    logger.add('logs/strategy.log', rotation='100mb', retention='5 days', level='DEBUG')
    st = Strategy(
        ib_host=config['ib']['host'],
        ib_port=config['ib']['port'],
        asset_symbol=config['asset']['symbol'],
        asset_security_type=config['asset']['security_type'],
        asset_exchange=config['asset']['exchange'],
        asset_currency=config['asset']['currency'],
        asset_data_type=config['asset']['data_type'],
        timeframe=config['strategy']['timeframe'],
        cci_length=config['strategy']['cci_length'],
        save_data_to_csv=config['data']['save_data_to_csv'],
        save_data_to_csv_interval=config['data']['save_data_to_csv_interval'],
        order_send_interval=config['data']['order_send_interval'],
        cci_long_entry_threshold=config['strategy']['cci_long_entry_threshold'],
        cci_long_exit_threshold=config['strategy']['cci_long_exit_threshold'],
        cci_short_entry_threshold=config['strategy']['cci_short_entry_threshold'],
        cci_short_exit_threshold=config['strategy']['cci_short_exit_threshold'],
        order_size=config['strategy']['order_size']
    )
    st.run()
