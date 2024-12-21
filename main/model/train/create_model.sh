#!/bin/bash
set -eu

# create data
python preproc.py --dir ./data/2020/ --gt --dfsave df_2020.pickle
python preproc.py --dir ./data/2021/ --gt --dfsave df_2021.pickle
python preproc.py --dir ./data/2022/ --gt --dfsave df_2022.pickle
python preproc.py --dir ./data/2023/ --gt --dfsave df_2023.pickle
# preprocess
python preproc.py --dfload df_2022.pickle --dftest df_2023.pickle --cutv --cutt --cuta --cutc --njob 32 --mlsave ./tmp/
# compact
# python preproc.py --dfload df_2020_2022.pickle --compact --dfsave df_2020_2022.pickle.compact --mlload base/mlmanager.XXX.pickle 
python preproc.py --dfload df_2020.pickle --compact --dfsave df_2020.pickle.compact --mlload base/mlmanager.XXX.pickle
python preproc.py --dfload df_2021.pickle --compact --dfsave df_2021.pickle.compact --mlload base/mlmanager.XXX.pickle
python preproc.py --dfload df_2022.pickle --compact --dfsave df_2022.pickle.compact --mlload base/mlmanager.XXX.pickle
python preproc.py --dfload df_2023.pickle --compact --dfsave df_2023.pickle.compact --mlload base/mlmanager.XXX.pickle
# concat train data
python -c "import pandas as pd; pd.concat([pd.read_pickle('df_2020.pickle.compact'), pd.read_pickle('df_2021.pickle.compact'), pd.read_pickle('df_2022.pickle.compact')], axis=0, ignore_index=False, sort=False).sort_index().to_pickle('df_2020_2022.pickle.compact')"
# test training
python preproc.py --dfload df_2020_2022.pickle.compact --dftest df_2023.pickle.compact --train --njob 32 --ntree 1000 --depth 12 --ncv 3 --mlsave ./models/