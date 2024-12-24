import argparse, json
import pandas as pd
import numpy as np
from kkgbdt.model import KkGBDT
from kkmlmanager.manager import MLManager
from kklogger import set_logger


LOGGER = set_logger(__name__)


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("--json",    type=str)
    parser.add_argument("--dftrain", type=str)
    parser.add_argument("--dftest",  type=str)
    parser.add_argument("--mlsave",  type=str)
    parser.add_argument("--ans",     type=str)
    parser.add_argument("--iter",    type=int)
    parser.add_argument("--lr",      type=float)
    parser.add_argument("--njob",    type=int, default=1)
    parser.add_argument("--new",     action='store_true', default=False)
    args = parser.parse_args()
    LOGGER.info(f"args: {args}")
    # config
    c = json.load(open(args.json))
    if args.ans  is not None: c["config"]["gt"  ] = args.ans
    if args.iter is not None: c["config"]["iter"] = args.iter
    if args.lr   is not None: c["parameters"]["learning_rate"] = args.lr
    LOGGER.info(f"config: {c}")
    # load pickle
    df_train = pd.read_pickle(args.dftrain)
    df_test  = pd.read_pickle(args.dftest)
    # manager
    manager = MLManager(
        df_train.columns[:np.where(df_train.columns == "===")[0][0]].tolist(),
        c["config"]["gt"], n_jobs=args.njob
    )
    manager.proc_registry(dict_proc={
        "row": [
            f'"ProcCondition", "{x} >= 0"' for x in manager.columns_ans
        ],
        "exp": [
            '"ProcAsType", np.float32, batch_size=500, n_jobs=1, is_jobs_fix=True', 
            '"ProcToValues"', 
            '"ProcReplaceInf", posinf=float("nan"), neginf=float("nan")', 
        ],
        "ans": [
            '"ProcAsType", np.int32, n_jobs=1, is_jobs_fix=True',
            '"ProcToValues"',
            '"ProcReshape", (-1, )',
        ]
    })
    manager.set_model(
        KkGBDT, c["config"]["n_classes"], model_func_predict="predict",
        mode="lgb", n_jobs=args.njob, **c["parameters"], **c["config"]["kwargs_model"]
    )
    # model fit
    params_fit=f"""dict(
        loss_func="{c['config']['loss_func']}", num_iterations={c['config']['iter']},
        x_valid=_validation_x, y_valid=_validation_y, loss_func_eval={c['config']['loss_func_eval']}, 
        early_stopping_rounds={c['config']['early_stopping_rounds']}, early_stopping_name={c['config']['early_stopping_name']}, 
        sample_weight='balanced', categorical_features={np.where(np.isin(manager.columns, c["data"]["categorical_features"]))[0].tolist()}
    )"""
    manager.fit_cross_validation(
        df_train, n_split=c["config"]["ncv"], mask_split=None, cols_multilabel_split=(c["data"]["split_by"] + list(manager.columns_ans)),
        n_cv=c["config"]["ncv"], indexes_train=None, indexes_valid=None,
        params_fit=params_fit, params_fit_evaldict={},
        is_proc_fit_every_cv=False, is_save_cv_models=True, colname_sample_weight=None
    )
    if args.mlsave is not None:
        manager.save(f"{args.mlsave}", exist_ok=True, is_minimum=True)

