import argparse
import json
import logging
import os
from pathlib import Path
from typing import Callable
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

from src.container import ServiceContainer
from src.stages import Stage
from src.utils.app_utils import load_env_vars
from src.utils.log_utils import setup_logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


def run_experiment(storage, labels_path=None):
    if labels_path is not None:
        with open(labels_path) as f:
            labels = json.load(f)
        for record in labels:
            path = storage.parse_blob_path(record['metadata']['blob_path'])
            storage.touch_blobs(Stage.DIFF_RAW.value, path.company, path.policy, path.timestamp)
        return None
    else:
        for root, dirs, files in os.walk(DATA_DIR):
            for name in files:
                # TODO: there's also junk in this folder! bug
                run_experiment(storage, os.path.join(DATA_DIR, root, name))


def prompt_eval(icl, storage, outfile: str) -> None:
    gold = (icl.load_true_labels()
            .drop(columns=['practically_substantive_pred', 'legally_substantive_pred'])
            .rename(columns={'practically_substantive_true':'practically_substantive',
                             'legally_substantive_true':'legally_substantive'}))
    pred = icl.load_pred_labels()
    gold['blob_path'] = gold['blob_path'].apply(storage.parse_blob_path)
    gold['blob_path'] = gold['blob_path'].apply(lambda x: os.path.join(x.company, x.policy, x.timestamp))
    pred['blob_path'] = pred['blob_path'].apply(storage.parse_blob_path)
    pred['blob_path'] = pred['blob_path'].apply(lambda x: os.path.join(x.company, x.policy, x.timestamp))
    compare = gold.merge(pred, on=["blob_path"], how="left", suffixes=("_true","_pred"))

    totals = compare.groupby(['model_version'])['blob_path'].nunique().rename('n').reset_index()

    groups = ['model_version','prompt_version_pred','schema_version']
    valid_json = compare[compare.run_id.notna()].groupby(groups)['run_id'].nunique().rename('m').reset_index()
    valid_json_pct = totals.merge(valid_json)
    valid_json_pct['pct'] = (valid_json_pct['m'] / valid_json_pct['n']).round(2)
    valid_json_pct = valid_json_pct[groups + ['pct']]

    valid_schema = compare[compare.parse_error == False].groupby(groups).size().rename('m').reset_index()
    valid_schema_pct = totals.merge(valid_schema)
    valid_schema_pct['pct'] = (valid_schema_pct['m'] / valid_schema_pct['n']).round(2)
    valid_schema_pct = valid_schema_pct[groups + ['pct']]

    confusion = compare[compare.parse_error == False]
    if confusion.empty:
        return

    def compute_metric(x: pd.Series, metric: Callable, name: str) -> float:
        return metric(x['practically_substantive_true'], x['practically_substantive_pred']).rename(name).round(2)

    accuracy = confusion.groupby(groups).agg(compute_metric, metric=accuracy_score, name="accuracy")
    precision = confusion.groupby(groups).agg(compute_metric, metric=precision_score, name="precision")
    recall = confusion.groupby(groups).agg(compute_metric, metric=recall_score, name="recall")
    f1 = confusion.groupby(groups).agg(compute_metric, metric=f1_score, name="f1")

    semantic = pd.concat([accuracy, precision, recall, f1], axis=1).reset_index()

    html = "<h1>TOTALS:</h1>" + totals.to_html(index=False) + \
            "<h1>VALID JSON:</h1>" + valid_json_pct.to_html(index=False) + \
            "<h1>VALID SCHEMA:</h1>" + valid_schema_pct.to_html(index=False) + \
            "<h1>CONFUSION:</h1>" + semantic.to_html(index=False) + \
        "<h1>SCHEMA:</h1><p>" + ','.join(compare.columns.to_list()) + "</p>"

    with open(outfile, "w") as f:
        f.write(html)


if __name__ == "__main__":
    load_env_vars()

    logger = setup_logger(__name__, logging.DEBUG)
    logging.getLogger('azure').setLevel(logging.WARNING)

    container = ServiceContainer.create_real()

    parser = argparse.ArgumentParser()
    sub_parsers = parser.add_subparsers(required=True)
    exp_parser = sub_parsers.add_parser('exp', help='run experiments')
    exp_parser.add_argument("--labels_path", required=False)
    exp_parser.set_defaults(func=run_experiment)
    eval_parser = sub_parsers.add_parser('eval', help='run evals')
    eval_parser.set_defaults(func=prompt_eval)
    args = parser.parse_args()