import difflib
import json
import os
import time
from typing import Optional, cast

import argilla as rg  # type: ignore
from pydantic import BaseModel

from scripts.labeling.dataset import DatasetBase
from src.stages import Stage
from src.utils.metadata_utils import extract_stage_metadata


class SummaryV1Dataset(DatasetBase):
    def create_dataset(self, name):
        dataset = self.client.datasets(name)
        if dataset is not None:
            return dataset

        # Arguilla doesn't support the table tag!
        template = """
                <style>
                .container {
                    border: 1px solid #ddd;
                    font-family: sans-serif;
                }
                .row {
                    display: flex;
                    border-bottom: 1px solid #ddd;
                }
                .row:last-child {
                    border-bottom: none;
                }
                .column {
                    flex: 1;
                    padding: 8px;
                }
                .column:first-child {
                    border-right: 1px solid #ddd;
                }
                .diff_add {
                    background-color: #d4fcbc; /* light green */
                    color: #006400;            /* dark green text */
                }
                .diff_sub {
                    background-color: #fbb6b6; /* light red */
                    color: #8b0000;            /* dark red text */
                    /* text-decoration: line-through; */
                }
                </style>
                <strong>{{record.metadata.blob_path}}</strong>
                <div class="container">
                    <div class="header">
                        <div class="column">Index</div>
                        <div class="column">Before</div>
                        <div class="column">After</div>
                    </div>
                    {{#each record.fields.diff_html}}
                    <div class="row">
                        <div class="column">{{@key}}</div>
                        <div class="column">{{{this.before}}}</div>
                        <div class="column">{{{this.after}}}</div>
                    </div>
                    {{/each}}
                </div>
            """

        settings = rg.Settings(
            fields=[
                rg.CustomField(name="diff_html", title="ToS Diff", template=template),
                rg.TextField(name="model_output", title="LLM Prediction"),
            ],
            questions=[
                rg.LabelQuestion(name="legally_substantive", labels=["True", "False", "unsure"]),
                rg.LabelQuestion(name="practically_substantive", labels=["True", "False", "unsure"]),
                rg.TextQuestion(name="feedback", required=False)
            ],
            metadata=[
                rg.TermsMetadataProperty(name="brief_model_version"),
                rg.TermsMetadataProperty(name="brief_prompt_version"),
                rg.TermsMetadataProperty(name="brief_schema_version"),
                rg.TermsMetadataProperty(name="summary_model_version"),
                rg.TermsMetadataProperty(name="summary_prompt_version"),
                rg.TermsMetadataProperty(name="summary_schema_version"),
                rg.TermsMetadataProperty(name="blob_path"),
                rg.IntegerMetadataProperty(name="timestamp"),
            ]
        )

        dataset = rg.Dataset(name=name, settings=settings)
        dataset.create()

        return dataset


    def create_records(self, dataset, schema_version, prompt_version, max_examples=10):
        # Stream from blob storage
        blob_names = self.storage.list_blobs_nest()

        records = []
        for company, policies in blob_names[Stage.SUMMARY_CLEAN.value].items():
            for policy, timestamps in policies.items():
                for timestamp, files in timestamps.items():
                    for file in files:
                        # Only add records for specified versions
                        summ_name = os.path.join(Stage.SUMMARY_CLEAN.value, company, policy, timestamp, file)
                        metadata = self.storage.adapter.load_metadata(summ_name)
                        stage_metadata = extract_stage_metadata(metadata, stage=Stage.SUMMARY_CLEAN.value)
                        if stage_metadata['schema_version'] != schema_version or stage_metadata['prompt_version'] != prompt_version:
                            continue

                        # Defensive check
                        diff_name = os.path.join(Stage.DIFF_RAW.value, company, policy, f"{timestamp}.json")
                        if f"{timestamp}.json" not in blob_names[Stage.DIFF_RAW.value].get(company, {}).get(policy, {}):
                            print(f"Unexpected missing file: {diff_name}")
                            continue

                        # Skip if this file is already in the label dataset
                        filter_label = rg.Filter(("metadata.blob_path", "==", diff_name))
                        filtered_records = dataset.records(query=rg.Query(filter=filter_label)).to_list(flatten=True)
                        if filtered_records:
                            continue

                        # Skip if no actual diff
                        diff = self.template_diffs(diff_name)
                        if not diff:
                            continue

                        model_version = stage_metadata.get("model_version")

                        summary_txt = self.storage.load_text_blob(summ_name)
                        summary = json.loads(summary_txt)

                        records.append(rg.Record(
                            fields={
                                "diff_html": diff,
                                "model_output": summary_txt,
                            },
                            suggestions=[  # Pre-fill from model
                                rg.Suggestion("legally_substantive", value=str(summary['legally_substantive']['rating'])),
                                rg.Suggestion("practically_substantive", value=str(summary['practically_substantive']['rating'])),
                            ],
                            metadata=metadata | dict(blob_path=diff_name, timestamp=int(time.time()))
                        ))
                        if len(records) == max_examples:
                            dataset.records.log(records)
                            return

        if not records:
            print("No records added.")


    def template_diffs(self, diff_name) -> dict:
        diff_obj = self.storage.load_json_blob(diff_name)
        # xxx: argilla does NOT accept list values.
        # so must be a string-keyed dict
        diffs = {}
        for i,d in enumerate(diff_obj['diffs']):
            if d['tag'] != 'equal':
                before = '\n'.join(d['before'])
                after = '\n'.join(d['after'])
                before, after = self.render_diff_as_html(before, after)
                diffs[str(i)] = {"before": before, "after": after}
        return diffs


    def render_diff_as_html(self, before: str, after: str) -> tuple[str, str]:
        """
        Returns two strings with span-level diff highlighting.
        """
        sm = difflib.SequenceMatcher(None, before, after)
        before_result = []
        after_result = []

        for tag, i1, i2, j1, j2 in sm.get_opcodes():
            a = before[i1:i2]
            b = after[j1:j2]

            if tag == 'equal':
                before_result.append(a)
                after_result.append(b)
            elif tag == 'replace':
                before_result.append(f'<span class="diff_sub">{a}</span>')
                after_result.append(f'<span class="diff_add">{b}</span>')
            elif tag == 'delete':
                before_result.append(f'<span class="diff_sub">{a}</span>')
            elif tag == 'insert':
                after_result.append(f'<span class="diff_add">{b}</span>')

        return ''.join(before_result), ''.join(after_result)


class SummaryLabelBase(BaseModel):
    ...

def optional_bool(value: str) -> Optional[bool]:
    return {'True': True, 'False': False, "unsure": None}.get(value, None)

class SummaryLabelV1(SummaryLabelBase):
    practically_substantive_true: Optional[bool]
    practically_substantive_pred: bool

    @classmethod
    def from_dict(cls, label: dict):
        pst = label['responses'].get('practically_substantive', [{}])[0].get('value')
        psp = label['suggestions']['practically_substantive']['value']
        pst = optional_bool(pst)
        psp = cast(bool, optional_bool(psp))
        return SummaryLabelV1(
            practically_substantive_true=pst,
            practically_substantive_pred=psp
        )

class SummaryLabelV2(SummaryLabelV1):
    feedback: str

    @classmethod
    def from_dict(cls, label: dict):
        v1 = SummaryLabelV1.from_dict(label)
        feedback = label['responses'].get('feedback', [{}])[0].get('value', "")
        return cls(practically_substantive_true = v1.practically_substantive_true,
                   practically_substantive_pred = v1.practically_substantive_pred,
                   feedback = feedback)