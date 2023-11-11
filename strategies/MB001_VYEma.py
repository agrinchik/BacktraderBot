import backtrader.indicators as btind
from strategies.genericstrategy import GenericStrategy


class MB001_MoonBotVYEmaStrategy(GenericStrategy):
    '''
    This is an implementation of a MoonBot EMA strategy based on VY's formula - for 1m timeframe:
    MAX(35m(25-40m), 1) < -1.4(-2..-6) AND MIN(5m, 1) > 0.2(0.4) AND EMA(6h(8h), 1) > 0.1
    '''

    params = (
        ("debug", False),
        ("wfo_cycle_id", None),
        ("wfo_cycle_training_id", None),
        ("startcash", 10000),
        ("needlong", True),
        ("needshort", True),
        ("formula1time", 35),
        ("formula1pricedelta", -1.4),
        ("formula2pricedelta", 0.2),
        ("formula3pricedelta", 0.1),
        ("formula3time", 6 * 60),
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
        self.p.tp = abs(self.p.formula1pricedelta)
        super().__init__()
        self.hl2 = (self.data.high + self.data.low) / 2
        self.formula1_max = btind.Highest(self.data.high, period=self.p.formula1time)
        self.formula2_min = btind.Lowest(self.data.low, period=5)
        self.formula3_sma = btind.SimpleMovingAverage(self.hl2, period=2)

        self.formula1_val = [0]
        self.formula2_val = [0]
        self.formula3_val = [0]

    def pct_val(self, value1, value2):
        return 100 * (value2 - value1) / value1

    def calculate_signals(self):
        #Signals
        self.formula1_val.append(self.pct_val(self.formula1_max[0], self.hl2[0]))
        self.formula2_val.append(self.pct_val(self.formula2_min[0], self.hl2[0]))
        self.formula3_val.append(self.pct_val(self.formula3_sma[-self.p.formula1time], self.hl2[0]))

        self.up1 = True if self.formula1_val[-1] < self.p.formula1pricedelta and self.formula2_val[-1] > self.p.formula2pricedelta and self.formula3_val[-1] > self.p.formula3pricedelta else False

        self.is_open_long = True if self.up1 else False
        self.is_close_long = False
        self.is_open_short = False
        self.is_close_short = False

    def print_strategy_debug_info(self):
        self.log('self.hl2[0] = {}'.format(self.hl2[0]))
        self.log('self.formula1_max[0] = {}'.format(self.formula1_max[0]))
        self.log('self.formula2_min[0] = {}'.format(self.formula2_min[0]))
        self.log('self.formula3_sma[0] = {}'.format(self.formula3_sma[0]))
        self.log('self.formula1_val[-1] = {}'.format(self.formula1_val[-1]))
        self.log('self.formula2_val[-1] = {}'.format(self.formula2_val[-1]))
        self.log('self.formula3_val[-1] = {}'.format(self.formula3_val[-1]))
        self.log('self.up1 = {}'.format(self.up1))




