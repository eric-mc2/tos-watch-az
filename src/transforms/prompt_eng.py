import json
from dataclasses import dataclass
from typing import Callable, Any

import pandas as pd
from pydantic import ValidationError
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from src.services.blob import BlobService
from src.stages import Stage
import os
from functools import lru_cache
from schemas.summary.registry import CLASS_REGISTRY

class PromptEng:
    storage: BlobService
    _cache: dict[str, Any]
    _cache_order: list[str]

    def __init__(self, storage: BlobService):
        self.storage = storage
        self._cache = {}
        self._cache_order = []
        if os.path.exists("data/substantive_v1/substantive_v1.json"):
            with open("data/substantive_v1/substantive_v1.json") as f:
                data = f.read()
                self.storage.upload_json_blob(data, f"{Stage.LABELS.value}/substantive_v1.json")

    def load_true_labels(self, label_blob: str="") -> pd.DataFrame:
        if not label_blob:
            gold_list = []
            for blob in self.storage.adapter.list_blobs():
                if blob.startswith(Stage.LABELS.value):
                    gold_list.append(self.load_true_labels(blob))
            return pd.concat(gold_list)
        else:
            gold_list = []
            if label_blob in self._cache:
                labels = self._cache[label_blob]
            else:
                labels = self.storage.load_json_blob(label_blob)
                if len(self._cache) >= 5:
                    del self._cache[self._cache_order.pop()]
                self._cache[label_blob] = labels
                self._cache_order.insert(0, label_blob)
            for label in labels:
                gold_list.append(label['metadata'] | dict(
                    practically_substantive_true = label['responses']['practically_substantive'][0]['value'],
                    legally_substantive_true = label['responses']['legally_substantive'][0]['value'],
                    practically_substantive_pred = label['suggestions']['practically_substantive']['value'],
                    legally_substantive_pred = label['suggestions']['legally_substantive']['value'],
                ))
            gold = pd.DataFrame.from_records(gold_list)  # type: ignore
            gold['practically_substantive_true'] = gold['practically_substantive_true'].map({'True':1,'False':0})
            gold['legally_substantive_true'] = gold['legally_substantive_true'].map({'True':1,'False':0})
            gold['practically_substantive_pred'] = gold['practically_substantive_pred'].map({'True':1,'False':0})
            gold['legally_substantive_pred'] = gold['legally_substantive_pred'].map({'True':1,'False':0})
            gold = gold.dropna()
            return gold
    
    
    def load_pred_labels(self) -> pd.DataFrame:
        blobs = self.storage.adapter.list_blobs()
        clean_blobs = [b for b in blobs if b.startswith(Stage.SUMMARY_CLEAN.value)]
        pred_list = []
        for blob in clean_blobs:
            path = self.storage.parse_blob_path(blob)
            if path.run_id == "latest":
                continue
            key = os.path.join(Stage.DIFF_RAW.value, path.company, path.policy, path.timestamp + ".json")
            meta = self.storage.adapter.load_metadata(blob)
    
            schema = CLASS_REGISTRY[meta['schema_version']]
            summary_raw = self.storage.load_json_blob(blob)
            try:
                summary = schema(**summary_raw)
                parse_error = False
            except (ValidationError, TypeError):
                parse_error = True
            pred_list.append(meta | dict(
                blob_path = key,
                parse_error = parse_error,
                practically_substantive = summary.practically_substantive.rating if not parse_error else None,
            ))
        pred = pd.DataFrame.from_records(pred_list)  # type: ignore
        pred['practically_substantive'] = pred['practically_substantive'].map({True:1.0,False:0.0})
        return pred
    
    def prompt_eval(self) -> str:
        gold = (self.load_true_labels().drop(columns=['practically_substantive_pred','legally_substantive_pred'])
                .rename(columns={'practically_substantive_true':'practically_substantive',
                                 'legally_substantive_true':'legally_substantive'}))
        pred = self.load_pred_labels()
        gold['blob_path'] = gold['blob_path'].apply(self.storage.parse_blob_path)
        gold['blob_path'] = gold['blob_path'].apply(lambda x: os.path.join(x.company, x.policy, x.timestamp))
        pred['blob_path'] = pred['blob_path'].apply(self.storage.parse_blob_path)
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
            return "No matching predictions x labels."
    
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
        return html
    
    def run_experiment(self, labels_name = None):
        if labels_name is not None:
            labels = self.storage.load_json_blob(labels_name)
            for record in labels:
                path = self.storage.parse_blob_path(record['metadata']['blob_path'])
                self.storage.touch_blobs(Stage.DIFF_RAW.value, path.company, path.policy, path.timestamp)
            return None
        else:
            blobs = self.storage.adapter.list_blobs()
            label_names = [b for b in blobs if b.startswith(Stage.LABELS.value)]
            for b in label_names:
                self.run_experiment(b)