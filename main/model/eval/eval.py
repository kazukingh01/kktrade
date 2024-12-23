import argparse, datetime
import pandas as pd
import numpy as np
from kkmlmanager.manager import load_manager
from kklogger import set_logger


LOGGER     = set_logger(__name__)
THRE_BUY   = 0.40
THRE_SELL  = 0.40
FEE_TAKER  = 0.055 / 100.0
FEE_MAKER  = 0.020 / 100.0
CLS_BUY    = [6, 7, 8]
CLS_SELL   = [0, 1, 2]
RATIO_WITHIN     = FEE_TAKER
RATIO_ENTRY_BUY  = 1 - FEE_TAKER
RATIO_ENTRY_SELL = 1 + FEE_TAKER
RATIO_CLOSE      = 0.003


class Position:
    def __init__(self):
        self.price_ave   = 0
        self.volume      = 0
        self.amount      = 0
        self.fees        = 0
        self.limits_buy  = []
        self.limits_sell = []
        self.fee_taker   = 0.055 / 100.0
        self.fee_maker   = 0.020 / 100.0
    def set_limit_buy(self, price: float, target_price: float, size: int | float, lifetime: int=None, stop_price: float=None):
        LOGGER.info(f"price: {price}, target_price: {target_price}, size: {size}, lifetime: {lifetime}, stop_price: {stop_price}")
        assert isinstance(price, float) and price > 0
        assert isinstance(target_price, float) and target_price > 0
        assert isinstance(size, int) or isinstance(size, float)
        assert size > 0
        assert lifetime is None or (isinstance(lifetime, int) and lifetime > 0)
        assert stop_price is None or (isinstance(stop_price, float) and stop_price > 0)
        assert price > target_price
        if stop_price is not None:
            assert price < stop_price
        self.limits_buy.append((target_price, size, lifetime, stop_price))
    def set_limit_sell(self, price: float, target_price: float, size: int | float, lifetime: int=None, stop_price: float=None):
        LOGGER.info(f"price: {price}, target_price: {target_price}, size: {size}, lifetime: {lifetime}, stop_price: {stop_price}")
        assert isinstance(price, float) and price > 0
        assert isinstance(target_price, float) and target_price > 0
        assert isinstance(size, int) or isinstance(size, float)
        assert size > 0
        assert lifetime is None or (isinstance(lifetime, int) and lifetime > 0)
        assert stop_price is None or (isinstance(stop_price, float) and stop_price > 0)
        assert price < target_price
        if stop_price is not None:
            assert price > stop_price
        self.limits_sell.append((target_price, size, lifetime, stop_price))
    def step(self, price_open: float, price_high: float, price_low: float):
        limits_buy = []
        for price, size, lifetime, stop_price in self.limits_buy:
            assert price < price_open
            if price_low <= price <= price_high:
                LOGGER.info("!!!!! LIMIT order !!!!!", color=["BOLD", "GREEN"])
                self.buy(price, size, is_taker=False)
            elif stop_price is not None and price_low <= stop_price <= price_high:
                LOGGER.info("!!!!! STOP order !!!!!",  color=["BOLD", "YELLOW"])
                self.buy(stop_price, size, is_taker=True)
            else:
                if lifetime is None:
                    limits_buy.append((price, size, lifetime, stop_price))
                elif lifetime is not None and lifetime > 1:
                    limits_buy.append((price, size, lifetime - 1, stop_price))
        limits_sell = []
        for price, size, lifetime, stop_price in self.limits_sell:
            assert price > price_open
            if price_low <= price <= price_high:
                LOGGER.info("!!!!! LIMIT order !!!!!", color=["BOLD", "GREEN"])
                self.sell(price, size, is_taker=False)
            elif stop_price is not None and price_low <= stop_price <= price_high:
                LOGGER.info("!!!!! STOP order !!!!!",  color=["BOLD", "YELLOW"])
                self.sell(stop_price, size, is_taker=True)
            else:
                if lifetime is None:
                    limits_sell.append((price, size, lifetime, stop_price))
                elif lifetime is not None and lifetime > 1:
                    limits_sell.append((price, size, lifetime - 1, stop_price))
        self.limits_buy  = limits_buy
        self.limits_sell = limits_sell
    def buy(self, price: float, size: int | float, is_taker: bool=False):
        assert isinstance(price, float) and price > 0
        assert isinstance(size, int) or isinstance(size, float)
        assert size > 0
        if self.size >= 0:
            self.price_ave = (self.price_ave * abs(self.size) + price * size) / (abs(self.size) + size)
            LOGGER.info(f"price: {price}, size: {size}, is_taker: {is_taker}, price_ave: {self.price_ave}")
        else:
            if (self.size + size) <= 0:
                amount         = (self.price_ave - price) * size
            else:
                amount         = (self.price_ave - price) * self.size
                self.price_ave = price
            self.amount += amount
            LOGGER.info(f"price: {price}, size: {size}, is_taker: {is_taker}, price_ave: {self.price_ave}, amount: {amount}")
        self.size       += size
        self.fees       += (self.fee_taker if is_taker else self.fee_maker) * size * price
    def sell(self, price: float, size: int | float, is_taker: bool=False):
        assert isinstance(price, float) and price > 0
        assert isinstance(size, int) or isinstance(size, float)
        assert size > 0
        if self.size <= 0:
            self.price_ave  = (self.price_ave * abs(self.size) + price * size) / (abs(self.size) + size)
            LOGGER.info(f"price: {price}, size: {size}, is_taker: {is_taker}, price_ave: {self.price_ave}")
        else:
            if (self.size - size) >= 0:
                amount         = (price - self.price_ave) * size
            else:
                amount         = (price - self.price_ave) * self.size
                self.price_ave = price
            self.amount += amount
            LOGGER.info(f"price: {price}, size: {size}, is_taker: {is_taker}, price_ave: {self.price_ave}, amount: {amount}")
        self.size       -= size
        self.fees       += (self.fee_taker if is_taker else self.fee_maker) * size * price
    def close_all_positions(self, price: float):
        if self.size > 0:
            self.sell(price, self.size, is_taker=True)
        elif self.size < 0:
            self.buy(price, -1 * self.size, is_taker=True)


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("--dfload", type=str)
    parser.add_argument("--mlload", type=str)
    parser.add_argument("--dfsave", type=str)
    parser.add_argument("--njob",   type=int, default=1)
    args = parser.parse_args()
    LOGGER.info(f"args: {args}")
    assert args.dfload is not None
    if args.mlload is not None:
        df = pd.read_pickle(args.dfload)
        manager = load_manager(args.mlload, args.njob)
        output, input_y, input_index = manager.predict(df=df)
        COL_ANS             = manager.columns_ans[0].replace("cls_", "close_")
        colname_base_price  = COL_ANS.split("_")[0] + "_120_" + COL_ANS.split("_")[-1]
        colname_entry_price = (COL_ANS.split("_")[0] + "_in120_120_" + COL_ANS.split("_")[-1])
        colname_high_price  = COL_ANS.replace("close_", "high_")
        colname_low_price   = COL_ANS.replace("close_", "low_")
        df_pred = pd.DataFrame(output, columns=[f"pred_{x}" for x in range(output.shape[-1])], index=input_index)
        df_pred[[COL_ANS, colname_base_price, colname_entry_price, colname_high_price, colname_low_price]] = df[[COL_ANS, colname_base_price, colname_entry_price, colname_high_price, colname_low_price]].copy()
        if args.dfsave is not None:
            df_pred.to_pickle(f"{args.dfsave}")
    else:
        df_pred = pd.read_pickle(args.dfload)
        COL_ANS, colname_base_price, colname_entry_price, colname_high_price, colname_low_price = df_pred.columns[-5:]
    """
                                                                pred_X
                                                          colname_high_price
                                                          colname_low_price
       -240                   -120                     0           ^         120                    240
    -----+----------------------+----------------------+-----------|----------+-----------|----------+
                                v                      v
                        colname_base_price     colname_entry_price     
    """
    df_pred = df_pred.sort_index()
    df_pred["pred_sell"] = df_pred[[f"pred_{x}" for x in CLS_SELL]].sum(axis=1)
    df_pred["pred_buy" ] = df_pred[[f"pred_{x}" for x in CLS_BUY ]].sum(axis=1)
    df_pred["is_cond_pred_sell"] = (df_pred["pred_sell"] >= THRE_SELL)
    df_pred["is_cond_pred_buy"]  = (df_pred["pred_buy"]  >= THRE_BUY )
    boolwk = (df_pred["is_cond_pred_sell"] & df_pred["is_cond_pred_buy"]) & (df_pred["pred_sell"] > df_pred["pred_buy"])
    df_pred.loc[boolwk, "is_cond_pred_sell"] = True
    df_pred.loc[boolwk, "is_cond_pred_buy" ] = False
    pos  = Position()
    SIZE = 0.001
    for x_index, (price_base, price_entry, price_high, price_low, is_cond_pred_sell, is_cond_pred_buy) in zip(df_pred.index, df_pred[[colname_base_price, colname_entry_price, colname_high_price, colname_low_price, "is_cond_pred_sell", "is_cond_pred_buy"]].values):
        if np.isnan(price_base) or np.isnan(price_entry): continue
        is_sell, is_buy = False, False
        strdate = datetime.datetime.fromtimestamp(x_index, tz=datetime.UTC).strftime("%Y-%m-%d %H:%M")
        LOGGER.info(f"{strdate}, price: {price_entry}")
        if is_cond_pred_sell:
            if (1 - RATIO_WITHIN) < (price_entry / price_base) < (1 + RATIO_WITHIN):
                is_sell = True
        elif is_cond_pred_buy:
            if (1 - RATIO_WITHIN) < (price_entry / price_base) < (1 + RATIO_WITHIN):
                is_buy = True
        if is_sell:
            pos.sell(price_entry, SIZE, is_taker=True)
            pos.set_limit_buy( price_entry, price_base * (1 - RATIO_CLOSE), SIZE, lifetime=None, stop_price=price_base * (1 + (RATIO_CLOSE * 2)))
        elif is_buy:
            pos.buy(price_entry, SIZE, is_taker=True)
            pos.set_limit_sell(price_entry, price_base * (1 + RATIO_CLOSE), SIZE, lifetime=None, stop_price=price_base * (1 - (RATIO_CLOSE * 2)))
        pos.step(price_entry, price_high, price_low)
    pos.close_all_positions(price_base)
