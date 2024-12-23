import os, argparse
import pandas as pd
import numpy as np
from functools import partial
from kkmlmanager.manager import MLManager
from kkmlmanager.optuna import create_study
from kkgbdt.tune import tune_parameter
from kklogger import set_logger


LOGGER = set_logger(__name__)


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("--dfload", type=str)
    parser.add_argument("--dftest", type=str)
    parser.add_argument("--ans",    type=str)
    parser.add_argument("--njob",  type=int, default=1)
    parser.add_argument("--iter",  type=int, default=500)
    parser.add_argument("--new",   action='store_true', default=False)

    args = parser.parse_args()
    LOGGER.info(f"args: {args}")

    # load pickle
    df_train = pd.read_pickle(args.dfload)
    df_test  = pd.read_pickle(args.dftest)
    # manager
    manager = MLManager(df_train.columns[:np.where(df_train.columns == "===")[0][0]].tolist(), args.ans, n_jobs=args.njob)
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
    # pre process
    train_x, train_y, train_index = manager.proc_fit( df_train, is_row=True, is_exp=True, is_ans=True)
    valid_x, valid_y, valid_index = manager.proc_call(df_test,  is_row=True, is_exp=True, is_ans=True)
    # optuna
    params_const = {
        "learning_rate"    : args.lr,
        "num_leaves"       : 1000,
        "is_gpu"           : False,
        "random_seed"      : 0,
        "max_depth"        : 0,
        "subsample"        : 1,
        "colsample_bylevel": 1,
        "max_bin"          : 64,
        "min_data_in_bin"  : 5,
    }
    params_search='''{
        "min_child_weight" : trial.suggest_float("min_child_weight", 1e-3, 10.0, log=True),
        "colsample_bytree" : trial.suggest_float("colsample_bytree", 0.01, 0.25),
        "colsample_bynode" : trial.suggest_float("colsample_bynode", 0.20, 1.0),
        "reg_alpha"        : trial.suggest_float("reg_alpha",  1e-4, 10.0, log=True),
        "reg_lambda"       : trial.suggest_float("reg_lambda", 1e-2, 100.0, log=True),
        "min_split_gain"   : trial.suggest_float("min_split_gain", 1e-7, 1.0, log=True),
        "min_child_samples": trial.suggest_int("min_child_samples", 2, 100),
        "path_smooth"      : trial.suggest_float("path_smooth", 1e-4, 1e2, log=True),
    }'''

    func = partial(tune_parameter,
        mode="lgb", num_class=9, n_jobs=args.njob,
        eval_string=f'model.evals_result["valid_0"]["multi_logloss"][model.booster.best_iteration - 1]',
        x_train=train_x, y_train=train_y, loss_func="multiclass", num_iterations=args.iter,
        x_valid=valid_x, y_valid=valid_y, loss_func_eval="multiclass",
        early_stopping_rounds=5, early_stopping_name=0, stopping_val=None, stopping_rounds=10, 
        stopping_is_over=True, stopping_train_time=None, sample_weight="balanced", categorical_features=None,
        params_const=params_const, params_search=params_search,
    )
    storage = "tune.db"
    if args.new and os.path.exists(storage): os.remove(storage)
    study = create_study(func, args.trials, storage=f'sqlite:///{storage}', is_new=args.new, name="trade")
