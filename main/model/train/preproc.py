import glob, argparse
import pandas as pd
import numpy as np
# local package
# from kkgbdt.model import KkGBDT
from kkmlmanager.manager import MLManager, load_manager
from kklogger import set_logger
from scipy.stats import norm


LOGGER      = set_logger(__name__)
BASE_SR     = 120
BASE_ITVLS  = [0, 120, 480, 2400]
SYMBOLS_ANS = [11,12,13,14,15,16]
COLUMNS_ANS = ["gt@cls_in240_120_s14", "gt@cls_in600_480_s14", "gt@cls_in2520_2400_s14"]
assert BASE_SR == BASE_ITVLS[1]


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("--dir",    type=str)
    parser.add_argument("--dfsave", type=str)
    parser.add_argument("--dfload", type=str)
    parser.add_argument("--dftest", type=str)
    parser.add_argument("--mlsave", type=str)
    parser.add_argument("--mlload", type=str)
    parser.add_argument("--njob", type=int, default=1)
    parser.add_argument("--gt",   action='store_true', default=False)
    parser.add_argument("--cutv", action='store_true', default=False)
    parser.add_argument("--cutt", action='store_true', default=False)
    parser.add_argument("--cuta", action='store_true', default=False)
    parser.add_argument("--cutc", action='store_true', default=False)
    parser.add_argument("--compact", action='store_true', default=False)
    parser.add_argument("--train",   action='store_true', default=False)
    args = parser.parse_args()
    LOGGER.info(f"args: {args}")
    if args.dfload is None:
        assert args.dir    is not None
        assert args.dfsave is not None
    else:
        assert args.dir    is None
        if args.compact: assert args.dfsave is not None
        else:            assert args.dfsave is None
    if args.compact:
        assert args.dfsave is not None
    if args.dfload is None:
        LOGGER.info(f"load pickles in a directory. [{args.dir}]")
        list_data = glob.glob(f"{args.dir}/*.pickle")
        df = pd.concat([pd.read_pickle(x) for x in list_data], axis=0, ignore_index=False, sort=False)
    else:
        LOGGER.info(f"load dataframe pickle [{args.dfload}]")
        df = pd.read_pickle(args.dfload)
    if args.gt:
        LOGGER.info(f"create ground truth.")
        ndf_sbls  = np.unique(df.columns[np.where(df.columns == "===")[0][0] + 1:].str.split("_").str[-1])
        ndf_itbls = np.unique(df.columns[np.where(df.columns == "===")[0][0] + 1:].str.split("_").str[-2]).astype(int)
        list_thre = [-float("inf"), 0.992, 0.996, 0.999, 1.001, 1.004, 1.008, float("inf")]
        for itvl in BASE_ITVLS:
            for sbl in ndf_sbls:
                df[f"gt@ave_in{  int(itvl + BASE_SR)}_{itvl}_{sbl}"] = df[f"gt@ave_{itvl}_{sbl}"].shift(-int((itvl + BASE_SR) // BASE_SR)) # predict in 2+2min, 8+2min and 40+2min. Additional 2min is prepared for processing of creating mart data.
                df[f"gt@ratio_in{int(itvl + BASE_SR)}_{itvl}_{sbl}"] = df[f"gt@ave_in{int(itvl + BASE_SR)}_{itvl}_{sbl}"] / df[f"gt@ave_{BASE_SR}_{sbl}"]
                sewk   = pd.cut(df[f"gt@ratio_in{int(itvl + BASE_SR)}_{itvl}_{sbl}"], list_thre)
                dictwk = {x:i for i, x in enumerate(np.sort(sewk[~sewk.isna()].unique()))}
                df[f"gt@cls_in{  int(itvl + BASE_SR)}_{itvl}_{sbl}"] = sewk.map(dictwk).astype(float).fillna(-1).astype(np.int32)
    if args.dfsave is not None and args.compact == False:
        LOGGER.info(f"save dataframe pickle [{args.dfsave}]")
        df.to_pickle(f"{args.dfsave}")
    if args.dftest is not None:
        LOGGER.info(f"load dataframe pickle [test] [{args.dftest}]")
        df_test = pd.read_pickle(args.dftest)
    else:
        df_test = None
    # preprocess
    if args.mlload is None:
        manager = MLManager(df.columns[:np.where(df.columns == "===")[0][0]].tolist(), COLUMNS_ANS[-1], n_jobs=args.njob)
    else:
        manager = load_manager(args.mlload, args.njob)
    if args.cutv:
        manager.cut_features_by_variance(df, cutoff=0.999, ignore_nan=False, batch_size=128)
    if args.cutt:
        manager.cut_features_by_randomtree_importance(df, cutoff=None, max_iter=5, min_count=1000, dtype=np.float16, batch_size=100, class_weight='balanced')
    if args.cuta:
        manager.cut_features_by_adversarial_validation(df, df_test, cutoff=None, n_split=3, n_cv=2, dtype=np.float16, batch_size=100)
    if args.cutc:
        manager.cut_features_by_correlation(df, cutoff=None, dtype='float32', is_gpu=True, corr_type='pearson',  sample_size=50000, batch_size=2000, min_n=100)
        manager.cut_features_by_correlation(df, cutoff=None, dtype='float32', is_gpu=True, corr_type='spearman', sample_size=50000, batch_size=2000, min_n=100)
    if args.mlsave is not None and args.train == False:
        manager.save(f"{args.mlsave}", exist_ok=True)
    # compact
    if args.compact:
        manager.cut_features_auto(
            list_proc = [
                f"self.cut_features_by_variance(cutoff=0.99, ignore_nan=False, batch_size=128)",
                f"self.cut_features_by_randomtree_importance(cutoff=0.95)",
                f"self.cut_features_by_adversarial_validation(cutoff=100, thre_count='mean')",
                f"self.cut_features_by_correlation(cutoff=0.92, corr_type='pearson')",
                f"self.cut_features_by_correlation(cutoff=0.92, corr_type='spearman')",
            ]
        )
        index_exp = np.where(df.columns == "===")[0][0]
        df.loc[:, manager.columns.tolist() + df.columns[index_exp:].tolist()].to_pickle(args.dfsave)
    # test train
    if args.train:
        for colname_ans in COLUMNS_ANS:
            LOGGER.info(f"Training [{colname_ans}]", color=["BOLD", "GREEN"])
            LOGGER.info(f"{df.groupby([colname_ans]).size()}")
            manager = MLManager(df.columns[:np.where(df.columns == "===")[0][0]].tolist(), colname_ans, n_jobs=args.njob)
            manager.fit_basic_treemodel(df, df_valid=None, df_test=df_test, n_estimators=1000)
            if args.mlsave is not None:
                manager.save(f"{args.mlsave}", exist_ok=True)
