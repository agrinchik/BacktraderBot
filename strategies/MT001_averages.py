import backtrader as bt
import backtrader.indicators as btind
from strategies.genericstrategy import GenericStrategy


class MT001_MoonTraderAveragesStrategy(GenericStrategy):
    '''
    This is an implementation of a scalping MoonTrader's Averages strategy for tick timeframe
    '''

    params = (
        ("debug", False),
        ("wfo_cycle_id", None),
        ("wfo_cycle_training_id", None),
        ("startcash", 10000),
        ("needlong", True),
        ("needshort", True),
        ("timeframe_ms", 1000),
        ("long_period", 60),
        ("short_period", 5),
        ("distance", 0.5),
        ("exitmode", None),
        ("sl", None),
        ("tslflag", None),
        ("tp", None),
        ("ttpdist", None),
        ("tbdist", None),
        ("numdca", None),
        ("dcainterval", None),
        ("fromyear", 2018),
        ("toyear", 2025),
        ("frommonth", 10),
        ("tomonth", 10),
        ("fromday", 1),
        ("today", 31),
    )

    def __init__(self):
        super().__init__()

        self.ema_diff_pct = [0.0, 0.0]

        self.hl2 = (self.data.high + self.data.low) / 2

        long_ema_period = int(self.p.long_period / (self.p.timeframe_ms / 1000))
        self.long_ema = btind.ExponentialMovingAverage(self.hl2, period=long_ema_period)
        short_ema_period = int(self.p.short_period / (self.p.timeframe_ms / 1000))
        self.short_ema = btind.ExponentialMovingAverage(self.hl2, period=short_ema_period)

    def calculate_signals(self):
        self.ema_diff_pct.append(100 * (self.short_ema[0] - self.long_ema[0]) / self.long_ema[0])

        self.is_open_long = True if self.ema_diff_pct[-2] > -self.p.distance and self.ema_diff_pct[-1] <= -self.p.distance else False
        self.is_close_long = False
        self.is_open_short = False
        self.is_close_short = False

    def print_strategy_debug_info(self):
        self.log('self.long_ema = {}'.format(self.long_ema[0]))
        self.log('self.short_ema = {}'.format(self.short_ema[0]))
        self.log('self.ema_diff_pct = {}'.format(self.ema_diff_pct[-1]))

