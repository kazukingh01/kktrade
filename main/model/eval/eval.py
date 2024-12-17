import argparse
import pandas as pd
from kkmlmanager.manager import MLManager, load_manager
from kklogger import set_logger


LOGGER = set_logger(__name__)
THRE_BUY   = 0.2
THRE_SELL  = 0.2
RATIO_BUY  = 1.002
RATIO_SELL = 0.998
COL_ANS    = "gt@cls_in2520_2400_s14"


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
    df_pred = pd.DataFrame(output, columns=[f"pred_{x}" for x in range(output.shape[-1])], index=input_index)
    df_pred["is_sell"] = (df_pred["pred_0"] + df_pred["pred_1"]) >= THRE_SELL
    df_pred["is_buy"]  = (df_pred["pred_5"] + df_pred["pred_6"]) >= THRE_BUY
    df_pred.loc[(df_pred["is_sell"] & df_pred["is_buy"]), "is_sell"] = False
    columns = ["ave_120_s14", "gt@close_in120_120_s14"]
    df_pred[columns] = df[columns].copy()
    df_pred["is_entry_sell"] = (df_pred["gt@ave_120_s14"] * RATIO_SELL < df_pred["gt@close_in120_120_s14"])
    df_pred["is_entry_buy" ] = (df_pred["gt@ave_120_s14"] * RATIO_BUY  > df_pred["gt@close_in120_120_s14"])
    df_pred["entry_price"] = float("nan")
    boolwk = (df_pred["is_sell"] & df_pred["is_entry_sell"])
    df_pred.loc[boolwk, "entry_price"] = df_pred.loc[boolwk, "gt@close_in120_120_s14"].copy()
    boolwk = (df_pred["is_buy" ] & df_pred["is_entry_buy" ])
    df_pred.loc[boolwk, "entry_price"] = df_pred.loc[boolwk, "gt@close_in120_120_s14"].copy()
    # df["__pred"]df["gt@ave_120_s14"] * 0.996
    # df["gt@ave_120_s14"] * 1.004
    # df[["gt@ave_120_s14", "gt@close_in120_120_s14"]]

