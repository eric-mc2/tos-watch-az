import pandas as pd
from pydantic import BaseModel, ValidationError
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from src.blob_utils import list_blobs, parse_blob_path, load_metadata, load_json_blob, touch_blobs
from src.stages import Stage
import os

class Substantive(BaseModel):
    rating: bool
    explanation: str

class Summary(BaseModel):
    legally_substantive: Substantive
    practically_substantive: Substantive
    change_keywords: list[str]
    subject_keywords: list[str]
    helm_keywords: list[str]


def load_true_labels(label_blob=None) -> pd.DataFrame:
    if label_blob is None:
        gold = []
        for blob in list_blobs():
            if blob.startswith(Stage.LABELS.value):
                gold.append(load_true_labels(blob))
        return pd.concat(gold)
    else:
        gold = []
        labels = load_json_blob(label_blob)
        for label in labels:
            gold.append(label['metadata'] | dict(
                practically_substantive_true = label['responses']['practically_substantive'][0]['value'],
                legally_substantive_true = label['responses']['legally_substantive'][0]['value'],
                practically_substantive_pred = label['suggestions']['practically_substantive']['value'],
                legally_substantive_pred = label['suggestions']['legally_substantive']['value'],
            ))
        gold = pd.DataFrame.from_records(gold)
        gold['practically_substantive_true'] = gold['practically_substantive_true'].map({'True':1,'False':0})
        gold['legally_substantive_true'] = gold['legally_substantive_true'].map({'True':1,'False':0})
        gold['practically_substantive_pred'] = gold['practically_substantive_pred'].map({'True':1,'False':0})
        gold['legally_substantive_pred'] = gold['legally_substantive_pred'].map({'True':1,'False':0})
        gold = gold.dropna()
        return gold


def load_pred_labels() -> pd.DataFrame:
    blobs = list_blobs()
    clean_blobs = [b for b in blobs if b.startswith(Stage.SUMMARY_CLEAN.value)]
    pred = []
    for blob in clean_blobs:
        path = parse_blob_path(blob)
        if path.run_id == "latest":
            continue
        key = os.path.join(Stage.DIFF.value, path.company, path.policy, path.timestamp + ".json")
        meta = load_metadata(blob)
        summary = load_json_blob(blob)
        try:
            summary = Summary(**summary)
            parse_error = False
        except (ValidationError, TypeError):
            parse_error = True
        pred.append(meta | dict(
            blob_path = key,
            parse_error = parse_error,
            practically_substantive = summary.practically_substantive.rating if not parse_error else None,
            legally_substantive = summary.legally_substantive.rating if not parse_error else None
        ))
    pred = pd.DataFrame.from_records(pred)
    pred['practically_substantive'] = pred['practically_substantive'].map({True:1.0,False:0.0})
    pred['legally_substantive'] = pred['legally_substantive'].map({True:1.0,False:0.0})
    return pred

def prompt_eval() -> str:
    gold = (load_true_labels().drop(columns=['practically_substantive_pred','legally_substantive_pred'])
            .rename(columns={'practically_substantive_true':'practically_substantive',
                             'legally_substantive_true':'legally_substantive'}))
    pred = load_pred_labels()
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
    accuracy = (confusion.groupby(groups).apply(lambda x: 
                    accuracy_score(x['practically_substantive_true'], x['practically_substantive_pred']))
                    .rename('accuracy').round(2))
    precision = (confusion.groupby(groups).apply(lambda x: 
                    precision_score(x['practically_substantive_true'], x['practically_substantive_pred']))
                    .rename('precision').round(2))
    recall = (confusion.groupby(groups).apply(lambda x: 
                    recall_score(x['practically_substantive_true'], x['practically_substantive_pred']))
                    .rename('recall').round(2))
    f1 = (confusion.groupby(groups).apply(lambda x: 
                    f1_score(x['practically_substantive_true'], x['practically_substantive_pred']))
                    .rename('f1').round(2))
    semantic = pd.concat([accuracy, precision, recall, f1], axis=1).reset_index()

    html = "<h1>TOTALS:</h1>" + totals.to_html(index=False) + \
            "<h1>VALID JSON:</h1>" + valid_json_pct.to_html(index=False) + \
            "<h1>VALID SCHEMA:</h1>" + valid_schema_pct.to_html(index=False) + \
            "<h1>CONFUSION:</h1>" + semantic.to_html(index=False) + \
        "<h1>SCHEMA:</h1><p>" + ','.join(compare.columns.to_list()) + "</p>"
    return html

def run_experiment(labels_name = None) -> None:
    if labels_name is not None:
        labels = load_json_blob(labels_name)
        for record in labels:
            path = parse_blob_path(record['metadata']['blob_path'])
            touch_blobs(Stage.DIFF.value, path.company, path.policy, path.timestamp)
        return None
    else:
        blobs = list_blobs()
        label_names = [b for b in blobs if b.startswith(Stage.LABELS.value)]
        for b in label_names:
            run_experiment(b)