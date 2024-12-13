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
    parser.add_argument("--dir",     type=str)
    parser.add_argument("--dfsave",  type=str)
    parser.add_argument("--dfload",  type=str)
    parser.add_argument("--manager", type=str)
    parser.add_argument("--save", action='store_true', default=False)
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
    # preprocess
    if args.manager is None:
        manager = MLManager(df.columns[:np.where(df.columns == "===")[0][0]].tolist(), "gt@cls_in240_120_s14", n_jobs=8)
        manager.cut_features_by_variance(df, cutoff=0.9, ignore_nan=False, batch_size=16)
        manager.cut_features_by_randomtree_importance(df, cutoff=None, max_iter=10, min_count=100)
        if args.save:
            manager.save("./mlmanager", exist_ok=True)
        manager.cut_features_by_correlation(df, cutoff=0.95, dtype='float32', is_gpu=False, corr_type='pearson', batch_size=1, min_n=100)
    else:
        manager = load_manager(args.manager, 8)
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
