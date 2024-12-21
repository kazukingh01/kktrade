import argparse, datetime
import pandas as pd
import numpy as np
from kkmlmanager.manager import load_manager
from kklogger import set_logger


LOGGER     = set_logger(__name__)
THRE_BUY   = 0.40
THRE_SELL  = 0.40
RATIO_ENTRY_BUY  = 1.00055
RATIO_ENTRY_SELL = 0.99945
RATIO_CLOSE_BUY  = 1.003
RATIO_CLOSE_SELL = 0.997
FEE_TAKER  = 0.055 / 100.0
FEE_MAKER  = 0.020 / 100.0
CLS_BUY    = [6, 7, 8]
CLS_SELL   = [0, 1, 2]

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
    list_entry, status, list_fees, list_return, count_thre, limit_buy, limit_sell = [], None, [], [], 0, None, None
    for x_index, (price_base, price_entry, price_high, price_low, is_cond_pred_sell, is_cond_pred_buy) in zip(df_pred.index, df_pred[[colname_base_price, colname_entry_price, colname_high_price, colname_low_price, "is_cond_pred_sell", "is_cond_pred_buy"]].values):
        if np.isnan(price_base) or np.isnan(price_entry): continue
        is_sell, is_buy = False, False
        strdate = datetime.datetime.fromtimestamp(x_index, tz=datetime.UTC).strftime("%Y-%m-%d %H:%M")
        if is_cond_pred_sell:
            if (price_base * RATIO_ENTRY_SELL) < price_entry:
                is_sell = True
        elif is_cond_pred_buy:
            if (price_base * RATIO_ENTRY_BUY) > price_entry:
                is_buy = True
        if status is None:
            # Entry
            if is_sell:
                assert len(list_entry) == 0
                LOGGER.info(f"{strdate}, SELL     !!!!!!")
                list_entry.append(price_entry)
                list_fees. append(FEE_TAKER)
                status     = "sell"
                limit_buy  = price_base * RATIO_CLOSE_SELL
                limit_sell = None
            elif is_buy:
                assert len(list_entry) == 0
                LOGGER.info(f"{strdate}, BUY      !!!!!!")
                list_entry.append(price_entry)
                list_fees. append(FEE_TAKER)
                status     = "buy"
                limit_buy  = None
                limit_sell = price_base * RATIO_CLOSE_BUY
        elif status == "sell":
            if is_sell:
                LOGGER.info(f"{strdate}, CONTINUE !!!!!!")
                list_entry.append(price_entry)
                list_fees. append(FEE_TAKER)
                limit_buy  = price_base * RATIO_CLOSE_SELL
                limit_sell = None
            elif is_buy or (count_thre >= 2):
                amount_ret = (np.array(list_entry) / price_entry - 1).sum()
                LOGGER.info(f"{strdate}, status: {status}, retrun: {amount_ret}")
                list_return.append(amount_ret)
                list_fees. append(FEE_TAKER)
                status = None
                list_entry = []
                count_thre = 0
                limit_buy  = None
                limit_sell = None
            else:
                count_thre += 1
        elif status == "buy":
            if is_sell or (count_thre >= 2):
                amount_ret = (price_entry / np.array(list_entry) - 1).sum()
                LOGGER.info(f"{strdate}, status: {status}, retrun: {amount_ret}")
                list_return.append(amount_ret)
                list_fees. append(FEE_TAKER)
                status = None
                list_entry = []
                count_thre = 0
                limit_buy  = None
                limit_sell = None
            elif is_buy:
                LOGGER.info(f"{strdate}, CONTINUE !!!!!!")
                list_entry.append(price_entry)
                list_fees. append(FEE_TAKER)
                limit_buy  = None
                limit_sell = price_base * RATIO_CLOSE_BUY
            else:
                count_thre += 1
        if   limit_buy is not None:
            if price_low <= limit_buy <= price_high:
                amount_ret = (np.array(list_entry) / limit_buy - 1).sum()
                LOGGER.info(f"{strdate}, status: {status}, retrun: {amount_ret}")
                list_return.append(amount_ret)
                list_fees. append(FEE_MAKER)
                status = None
                list_entry = []
                count_thre = 0
                limit_buy  = None
                limit_sell = None
        elif limit_sell is not None:
            if price_low <= limit_sell <= price_high:
                amount_ret = (limit_sell / np.array(list_entry) - 1).sum()
                LOGGER.info(f"{strdate}, status: {status}, retrun: {amount_ret}")
                list_return.append(amount_ret)
                list_fees. append(FEE_MAKER)
                status = None
                list_entry = []
                count_thre = 0
                limit_buy  = None
                limit_sell = None
    if (len(list_entry) > 0) and np.isnan(price_base) == False and np.isnan(price_entry) == False:
        if status == "sell":
            amount_ret = (np.array(list_entry) / price_entry - 1).sum()
            list_return.append(amount_ret)
            list_fees. append(FEE_TAKER)
        elif status == "buy":
            amount_ret = (price_entry / np.array(list_entry) - 1).sum()
            list_return.append(amount_ret)
            list_fees. append(FEE_TAKER)
    LOGGER.info(f"return: {sum(list_return)}, fee: {sum(list_fees)}")
