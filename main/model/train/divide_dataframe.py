import argparse
import pandas as pd
from iterstrat.ml_stratifiers import MultilabelStratifiedKFold
from kklogger import set_logger


LOGGER = set_logger(__name__)


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("--df", type=str)
    parser.add_argument("--nsplit", type=int, default=2)
    parser.add_argument("--cols",   type=lambda x: x.split(","), help="--cols gt@cls_in2520_2400_s14,hour", default="")
    parser.add_argument("--isave",  type=lambda x: [int(y) for y in x.split(",")], default="0")
    args = parser.parse_args()
    LOGGER.info(f"args: {args}")
    for x in args.isave: assert x in list(range(args.nsplit))
    df       = pd.read_pickle(args.df)
    splitter = MultilabelStratifiedKFold(n_splits=args.nsplit, shuffle=True, random_state=0)
    ndf_y    = df[args.cols].values
    indexes_train, indexes_valid = [], []
    for i_split, (index_train, index_valid) in enumerate(splitter.split(df.index, ndf_y)):
        LOGGER.info(f"Split: {i_split}. \ntrain indexes: {index_train}\nvalid indexes: {index_valid}")
        indexes_train.append(index_train)
        indexes_valid.append(index_valid)
    for x in args.isave:
        df.iloc[indexes_train[x]].to_pickle(f"{args.df}.train.{x}")
        df.iloc[indexes_valid[x]].to_pickle(f"{args.df}.valid.{x}")