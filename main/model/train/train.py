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
BASE_ITVLS  = [120, 480, 2400]
SYMBOLS_ANS = [11,12,13,14,15,16]
assert BASE_SR == BASE_ITVLS[0]


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("--dir",    type=str)
    parser.add_argument("--dfsave", type=str)
    parser.add_argument("--dfload", type=str)
    parser.add_argument("--dftest", type=str)
    parser.add_argument("--mlsave", type=str)
    parser.add_argument("--mlload", type=str)
    parser.add_argument("--njob", type=int, default=1)
    parser.add_argument("--cutv", action='store_true', default=False)
    parser.add_argument("--cutt", action='store_true', default=False)
    parser.add_argument("--cuta", action='store_true', default=False)
    parser.add_argument("--cutc", action='store_true', default=False)
    parser.add_argument("--compact", action='store_true', default=False)
    args = parser.parse_args()
    LOGGER.info(f"args: {args}")
    if args.dfload is None:
        assert args.dir is not None
    else:
        assert args.dir    is None
        assert args.dfsave is None
    if args.dfload is None:
        list_data = glob.glob(f"{args.dir}/*.pickle")
        df = pd.concat([pd.read_pickle(x) for x in list_data], axis=0, ignore_index=False, sort=False)
        ndf_sbls  = np.unique(df.columns[np.where(df.columns == "===")[0][0] + 1:].str.split("_").str[-1])
        ndf_itbls = np.unique(df.columns[np.where(df.columns == "===")[0][0] + 1:].str.split("_").str[-2]).astype(int)
        for itvl in BASE_ITVLS:
            for sbl in ndf_sbls:
                df[f"gt@ave_in{  int(itvl + BASE_SR)}_{itvl}_{sbl}"] = df[f"gt@ave_{itvl}_{sbl}"].shift(-int((itvl + BASE_SR) // BASE_SR)) # predict in 2+2min, 8+2min and 40+2min. Additional 2min is prepared for processing of creating mart data.
                df[f"gt@ratio_in{int(itvl + BASE_SR)}_{itvl}_{sbl}"] = df[f"gt@ave_in{int(itvl + BASE_SR)}_{itvl}_{sbl}"] / df[f"gt@close_{BASE_SR}_{sbl}"]
        # create answer
        columns_ans = [f"gt@ratio_in{int(itvl + BASE_SR)}_{itvl}_s{sbl}" for itvl in BASE_ITVLS for sbl in SYMBOLS_ANS]
        ndf_val     = df.loc[:, columns_ans].values.copy()
        ndf_ans     = np.ones(df.loc[:, columns_ans].shape) * -1
        ndf_mean    = df[columns_ans].mean().values.reshape(1, -1)
        ndf_sigma   = df[columns_ans].std(). values.reshape(1, -1)
        z_values    = [-float("inf")] + [norm.ppf(p) for p in [0.1, 0.3, 0.7, 0.9]] + [float("inf")]
        for i, x in enumerate(z_values[:-1]):
            ndf_bool = (ndf_val >= (ndf_mean + (x * ndf_sigma))) & (ndf_val < (ndf_mean + (z_values[i+1] * ndf_sigma)))
            ndf_ans[ndf_bool] = i
        df = pd.concat([df, pd.DataFrame(ndf_ans.astype(int), index=df.index, columns=pd.Index(columns_ans).str.replace("gt@ratio", "gt@cls"))], axis=1, ignore_index=False)
        if args.dfsave is not None:
            df.to_pickle(f"{args.dfsave}")
    else:
        df = pd.read_pickle(args.dfload)
    df_test = pd.read_pickle(args.dftest) if args.dftest is not None else None
    # preprocess
    if args.mlload is None:
        manager = MLManager(df.columns[:np.where(df.columns == "===")[0][0]].tolist(), "gt@cls_in240_120_s14", n_jobs=args.njob)
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
    if args.mlsave is not None:
        manager.save(f"{args.mlsave}", exist_ok=True)
    # compact
    if args.compact is not None:
        manager.cut_features_auto(
            list_proc = [
                f"self.cut_features_by_variance(cutoff=0.99, ignore_nan=False, batch_size=128)",
                f"self.cut_features_by_randomtree_importance(cutoff=0.95)",
                f"self.cut_features_by_adversarial_validation(cutoff=100, thre_count='mean')",
                f"self.cut_features_by_correlation(cutoff=0.92, corr_type='pearson')",
                f"self.cut_features_by_correlation(cutoff=0.92, corr_type='spearman')",
            ]
        )

    # set model
    # manager.set_model(
    #     KkGBDT, 5, model_func_predict="predict",
    #     mode=args.mode, n_jobs=manager.n_jobs, **c["params"], **c["tc"]["kwargs_model"]
    # )
    # # registry proc
    # manager.proc_registry(dict_proc={
    #     "row": [],
    #     "exp": [
    #         '"ProcAsType", np.float32, batch_size=25', 
    #         '"ProcToValues"', 
    #         '"ProcReplaceInf", posinf=float("nan"), neginf=float("nan")', 
    #     ],
    #     "ans": [
    #         '"ProcAsType", np.int32, n_jobs=1',
    #         '"ProcToValues"',
    #         '"ProcReshape", (-1, )',
    #     ]
    # })
    # # training
    # manager.fit(df_train, df_valid=df_valid, is_proc_fit=True, is_eval_train=True)
    # # calibration
    # manager.calibration(is_use_valid=True, n_bins=100)
    # manager.calibration(is_use_valid=True, n_bins=100, is_binary_fit=True)
    # # test evaluation
    # df, se = manager.evaluate(df_valid, is_store=False)
    # # cross validation
    # manager.fit_cross_validation(df_train, n_split=5, n_cv=3, is_proc_fit_every_cv=True, is_save_cv_models=True)
    # # calibration
    # manager.calibration_cv_model(n_bins=100)
    # manager.set_cvmodel(is_calib=True)
    # # test evaluation
    # df, se = manager.evaluate(df_valid, is_store=False)
