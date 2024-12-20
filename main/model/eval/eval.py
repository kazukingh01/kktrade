import argparse
import pandas as pd
import numpy as np
from kkmlmanager.manager import load_manager
from kklogger import set_logger


LOGGER     = set_logger(__name__)
THRE_BUY   = 0.35
THRE_SELL  = 0.35
RATIO_BUY  = 1.0011
RATIO_SELL = 0.9989
FEE_TAKER  = 0.055 / 100.0
FEE_MAKER  = 0.020 / 100.0
CLS_BUY    = [4, 5, 6]
CLS_SELL   = [0, 1, 2]

if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("--dfload", type=str)
    parser.add_argument("--mlload", type=str)
    parser.add_argument("--njob",   type=int, default=1)
    args = parser.parse_args()
    LOGGER.info(f"args: {args}")
    df = pd.read_pickle(args.dfload)
    manager = load_manager(args.mlload, args.njob)
    output, input_y, input_index = manager.predict(df=df)
    COL_ANS             = manager.columns_ans[0].replace("cls_", "ave_")
    colname_base_price  = COL_ANS.split("_")[0] + "_120_" + COL_ANS.split("_")[-1]
    colname_entry_price = (COL_ANS.split("_")[0] + "_in120_120_" + COL_ANS.split("_")[-1]).replace("ave_", "close_")
    colname_close_price = COL_ANS.replace("ave_", "close_")
    colname_low_price   = COL_ANS.replace("ave_", "low_")
    colname_high_price  = COL_ANS.replace("ave_", "high_")
    df_pred = pd.DataFrame(output, columns=[f"pred_{x}" for x in range(output.shape[-1])], index=input_index)
    df_pred["pred_sell"] = df_pred[[f"pred_{x}" for x in CLS_SELL]].sum(axis=1)
    df_pred["pred_buy" ] = df_pred[[f"pred_{x}" for x in CLS_BUY ]].sum(axis=1)
    df_pred["is_cond_pred_sell"] = (df_pred["pred_sell"] >= THRE_SELL)
    df_pred["is_cond_pred_buy"]  = (df_pred["pred_buy"]  >= THRE_BUY )
    boolwk = (df_pred["is_cond_pred_sell"] & df_pred["is_cond_pred_buy"]) & (df_pred["pred_sell"] > df_pred["pred_buy"])
    df_pred.loc[boolwk, "is_cond_pred_sell"] = True
    df_pred.loc[boolwk, "is_cond_pred_buy" ] = False
    df_pred[[colname_base_price, colname_entry_price]] = df[[colname_base_price, colname_entry_price]].copy()
    list_entry, status, list_fees, list_return = [], None, [], []
    for i_entry, (price_base, price_entry, is_cond_pred_sell, is_cond_pred_buy) in enumerate(df_pred[[colname_base_price, colname_entry_price, "is_cond_pred_sell", "is_cond_pred_buy"]].values):
        is_sell, is_buy = False, False
        if is_cond_pred_sell:
            if (price_base * RATIO_SELL) < price_entry:
                is_sell = True
        elif is_cond_pred_buy:
            if (price_base * RATIO_BUY) > price_entry:
                is_buy = True
        if is_sell:
            if status is None:
                assert len(list_entry) == 0
                LOGGER.info("SELL     !!!!!!")
                list_entry.append(price_entry)
                list_fees. append(FEE_TAKER)
                status = "sell"
            elif status == "sell":
                LOGGER.info("CONTINUE !!!!!!")
                list_entry.append(price_entry)
                list_fees. append(FEE_TAKER)
            elif status == "buy":
                amount_ret = ((np.array(list_entry) - price_entry) / price_entry).sum()
                LOGGER.info(f"i: {i_entry}, status: {status}, retrun: {amount_ret}")
                list_return.append(amount_ret)
                list_fees. append(FEE_TAKER)
                status = None
        elif is_buy:
            if status is None:
                assert len(list_entry) == 0
                LOGGER.info("BUY      !!!!!!")
                list_entry.append(price_entry)
                list_fees. append(FEE_TAKER)
                status = "buy"
            elif status == "sell":
                amount_ret = (-1 * (np.array(list_entry) - price_entry) / price_entry).sum()
                LOGGER.info(f"i: {i_entry}, status: {status}, retrun: {amount_ret}")
                list_return.append(amount_ret)
                list_fees. append(FEE_TAKER)
                status = None
            elif status == "buy":
                LOGGER.info("CONTINUE !!!!!!")
                list_entry.append(price_entry)
                list_fees. append(FEE_TAKER)
        if i_entry == (df_pred.shape[0] - 1):
            if status == "sell":
                amount_ret = (-1 * (np.array(list_entry) - price_entry) / price_entry).sum()
                list_return.append(amount_ret)
                list_fees. append(FEE_TAKER)
            elif status == "buy":
                amount_ret = ((np.array(list_entry) - price_entry) / price_entry).sum()
                list_return.append(amount_ret)
                list_fees. append(FEE_TAKER)
    LOGGER.info(f"return: {sum(list_return)}, fee: {sum(list_fees)}")
