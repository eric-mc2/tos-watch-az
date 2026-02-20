import argparse
import logging
import os
from pathlib import Path
from typing import Callable
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

from src.container import ServiceContainer
from src.transforms.icl import SummaryICL, BriefICL
from src.utils.app_utils import load_env_vars

logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('argilla').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
EVALS_DIR = DATA_DIR / "evals"

def main(storage, outfile: str, version: str="") -> None:

    # TODO: Start here. Ought to have enough to create baseline eval.

    if version == "summary_v1":
        icl = SummaryICL(storage)
    else:
        icl = BriefICL(storage)

    gold = icl.load_true_labels(version)
    cols_to_drop = ['practically_substantive_pred', 'legally_substantive_pred']
    cols_to_drop = [c for c in cols_to_drop if c in gold.columns]
    gold = (gold
            .drop(columns=cols_to_drop)
            .rename(columns={'practically_substantive_true':'practically_substantive',
                             'legally_substantive_true':'legally_substantive'}))
    pred = icl.load_pred_labels()
    gold['blob_path'] = gold['blob_path'].apply(storage.parse_blob_path)
    gold['blob_path'] = gold['blob_path'].apply(lambda x: os.path.join(x.company, x.policy, x.timestamp))
    pred['blob_path'] = pred['blob_path'].apply(storage.parse_blob_path)
    pred['blob_path'] = pred['blob_path'].apply(lambda x: os.path.join(x.company, x.policy, x.timestamp))
    compare = gold.merge(pred, on=["blob_path"], how="left", suffixes=("_true","_pred"))

    totals = compare.groupby(['model_version'])['blob_path'].nunique().rename('n').reset_index()

    # TODO: legacy tests assume schema version is not in pred or true ...
    #       briefer persists mismatched versions: true is from briefer and pred is from summary
    groups = ['model_version','prompt_version_pred','schema_version_true']
    valid_json = compare[compare['run_id'].notna()].groupby(groups)['run_id'].nunique().rename('m').reset_index()
    if valid_json.empty:
        return

    valid_json_pct = totals.merge(valid_json)
    valid_json_pct['pct'] = (valid_json_pct['m'] / valid_json_pct['n']).round(2)
    valid_json_pct = valid_json_pct[groups + ['pct']]

    # nb: pred labels used to have parse_error column from which to compute structural validity
    # valid_schema = compare[compare['parse_error'] == False].groupby(groups).size().rename('m').reset_index()
    valid_schema = compare.groupby(groups).size().rename('m').reset_index()
    valid_schema_pct = totals.merge(valid_schema)
    valid_schema_pct['pct'] = (valid_schema_pct['m'] / valid_schema_pct['n']).round(2)
    valid_schema_pct = valid_schema_pct[groups + ['pct']]

    # nb: pred labels used to have parse_error column from which to compute structural validity
    # confusion = compare[compare['parse_error'] == False]
    confusion = compare
    if confusion.empty:
        return

    def compute_metric(x: pd.DataFrame, groups, metric: Callable, name: str) -> pd.Series:
        wrapper = lambda y: metric(y['practically_substantive_true'], y['practically_substantive_pred'])
        return x.groupby(groups).apply(wrapper).rename(name).round(2)

    accuracy = compute_metric(confusion, groups=groups, metric=accuracy_score, name="accuracy")
    precision = compute_metric(confusion, groups=groups, metric=precision_score, name="precision")
    recall = compute_metric(confusion, groups=groups, metric=recall_score, name="recall")
    f1 = compute_metric(confusion, groups=groups, metric=f1_score, name="f1")

    semantic = pd.concat([accuracy, precision, recall, f1], axis=1).reset_index()

    html = "<h1>TOTALS:</h1>" + totals.to_html(index=False) + \
            "<h1>VALID JSON:</h1>" + valid_json_pct.to_html(index=False) + \
            "<h1>VALID SCHEMA:</h1>" + valid_schema_pct.to_html(index=False) + \
            "<h1>CONFUSION:</h1>" + semantic.to_html(index=False) + \
        "<h1>SCHEMA:</h1><p>" + ','.join(compare.columns.to_list()) + "</p>"

    out_path = os.path.join(EVALS_DIR, outfile)
    out_dir = os.path.dirname(out_path)
    os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w") as f:
        f.write(html)

if __name__ == "__main__":
    load_env_vars()
    container = ServiceContainer.create_real()

    parser = argparse.ArgumentParser()
    parser.add_argument("--outfile", required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()

    main(outfile=args.outfile, storage=container.storage, version=args.version)