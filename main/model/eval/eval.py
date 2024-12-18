import argparse
import pandas as pd
from kkmlmanager.manager import load_manager
from kklogger import set_logger


LOGGER = set_logger(__name__)
THRE_BUY         = 0.1
THRE_SELL        = 0.1
RATIO_ENTRY_BUY  = 1.002
RATIO_ENTRY_SELL = 0.998
RATIO_BUY        = 1.004
RATIO_SELL       = 0.996
COL_ANS          = "gt@ave_in2520_2400_s14"


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
    COL_ANS = manager.columns_ans[0].replace("cls_", "ave_")
    df_pred = pd.DataFrame(output, columns=[f"pred_{x}" for x in range(output.shape[-1])], index=input_index)
    df_pred["is_cond_pred_sell"] = (df_pred["pred_0"] + df_pred["pred_1"]) >= THRE_SELL
    df_pred["is_cond_pred_buy"]  = (df_pred["pred_5"] + df_pred["pred_6"]) >= THRE_BUY
    df_pred.loc[(df_pred["is_cond_pred_sell"] & df_pred["is_cond_pred_buy"]), "is_cond_pred_sell"] = False
    colname_base_price  = COL_ANS.split("_")[0] + "_120_" + COL_ANS.split("_")[-1]
    colname_close_price = COL_ANS.replace("ave_", "close_")
    colname_low_price   = COL_ANS.replace("ave_", "low_")
    colname_high_price  = COL_ANS.replace("ave_", "high_")
    colname_entry_price = (COL_ANS.split("_")[0] + "_in120_120_" + COL_ANS.split("_")[-1]).replace("ave_", "close_")
    df_pred[colname_entry_price]  = df[colname_entry_price].copy()
    df_pred[colname_close_price]  = df[colname_close_price].copy()
    df_pred["is_cond_price_sell"] = (df[colname_base_price] * RATIO_ENTRY_SELL < df[colname_entry_price])
    df_pred["is_cond_price_buy" ] = (df[colname_base_price] * RATIO_ENTRY_BUY  > df[colname_entry_price])
    df_pred["target_price_sell"]  = df[colname_base_price] * RATIO_SELL
    df_pred["target_price_buy" ]  = df[colname_base_price] * RATIO_BUY
    df_pred["is_entry_sell"]      = (df_pred["is_cond_pred_sell"] & df_pred["is_cond_price_sell"])
    df_pred["is_entry_buy" ]      = (df_pred["is_cond_pred_buy" ] & df_pred["is_cond_price_buy" ])
    df_pred["entry_price"]        = float("nan")
    df_pred.loc[df_pred["is_entry_sell"], "entry_price"] = df_pred.loc[df_pred["is_entry_sell"], colname_entry_price].copy()
    df_pred.loc[df_pred["is_entry_buy" ], "entry_price"] = df_pred.loc[df_pred["is_entry_buy" ], colname_entry_price].copy()
    df_pred["exit_price"]       = float("nan")
    boolwk_ok = df_pred["is_entry_sell"] & (df_pred["target_price_sell"] >  df[colname_low_price])
    boolwk_ng = df_pred["is_entry_sell"] & (df_pred["target_price_sell"] <= df[colname_low_price])
    df_pred.loc[boolwk_ok, "exit_price"] = df_pred.loc[boolwk_ok, "target_price_sell"].copy()
    df_pred.loc[boolwk_ng, "exit_price"] = df_pred.loc[boolwk_ng, colname_close_price].copy()
    boolwk_ok = df_pred["is_entry_buy" ] & (df_pred["target_price_buy" ] <  df[colname_high_price])
    boolwk_ng = df_pred["is_entry_buy" ] & (df_pred["target_price_buy" ] >= df[colname_high_price])
    df_pred.loc[boolwk_ok, "exit_price"] = df_pred.loc[boolwk_ok, "target_price_buy"].copy()
    df_pred.loc[boolwk_ng, "exit_price"] = df_pred.loc[boolwk_ng, colname_close_price].copy()
    # df_pred["return"] = df_pred["entry_price"] - df_pred["exit_price"]
    df_pred["return"] = (df_pred["entry_price"] - df_pred[colname_close_price]) / df_pred["entry_price"]
    df_pred.loc[df_pred["is_entry_buy"], "return"] *= -1
    print(f"return: {df_pred["return"].sum() - ((~df_pred["return"].isna()).sum() * (0.055 / 100))}")
    # df["__pred"]df["gt@ave_120_s14"] * 0.996
    # df["gt@ave_120_s14"] * 1.004
    # df[["gt@ave_120_s14", "gt@close_in120_120_s14"]]

