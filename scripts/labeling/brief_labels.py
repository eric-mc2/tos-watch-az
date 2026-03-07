import json
import os
import time
from typing import Optional, cast, Self

import argilla as rg  # type: ignore
import logging
from random import shuffle

from pydantic import ValidationError, BaseModel

from schemas.brief.v2 import Brief, Memo
from scripts.labeling.dataset import DatasetBase
from src.stages import Stage
from src.utils.log_utils import setup_logger
from src.utils.metadata_utils import extract_stage_metadata

logger = setup_logger(__name__, logging.INFO)


class BriefV1Dataset(DatasetBase):
    metadata_fields = ["brief_model_version",
                "brief_prompt_version",
                "brief_schema_version",
                "summary_model_version",
                "summary_prompt_version",
                "summary_schema_version",
                "blob_path",
                "timestamp"]
    def create_dataset(self, name):
        dataset = self.client.datasets(name)
        if dataset is not None:
            return dataset

        # Argilla doesn't support the table tag!
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
                rg.TextField(name="memo_output", title="LLM Memos"),
                rg.TextField(name="summary_output", title="LLM Summary"),
            ],
            questions=[
                rg.LabelQuestion(name="practically_substantive", labels=["True", "False", "unsure"]),
                rg.LabelQuestion(name="notes_good", labels=["True", "False", "unsure"]),
                rg.TextQuestion(name="feedback", required=False)
            ],
            metadata=[rg.TermsMetadataProperty(name=field) for field in self.metadata_fields],
        )

        dataset = rg.Dataset(name=name, settings=settings)
        dataset.create()

        return dataset


    def create_records(self, dataset, schema_version, prompt_version, max_examples=10):
        blob_names = self.storage.adapter.list_blobs()
        shuffle(blob_names)

        records = []
        for blob_name in blob_names:
            parts = self.storage.parse_blob_path(blob_name)

            if parts.stage != Stage.BRIEF_CLEAN.value:
                continue

            # Only add records for specified versions
            memo_name = blob_name
            metadata = self.storage.adapter.load_metadata(memo_name)
            stage_metadata = extract_stage_metadata(metadata, stage=Stage.BRIEF_CLEAN.value)
            if stage_metadata['schema_version'] != schema_version or stage_metadata['prompt_version'] != prompt_version:
                continue

            # Defensive check
            diff_name = os.path.join(Stage.DIFF_SPAN.value, parts.company, parts.policy, f"{parts.timestamp}.json")
            if diff_name not in blob_names:
                logger.warning(f"Unexpected missing file: {diff_name}")
                continue

            # TODO: what if I don't want to test against latest?
            summ_name = os.path.join(Stage.SUMMARY_CLEAN.value, parts.company, parts.policy, parts.timestamp, "latest.json")
            if summ_name not in blob_names:
                logger.warning(f"Unexpected missing file: {summ_name}")
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

            # We actually want to use the summary metadata now because its more complete.
            metadata = self.storage.adapter.load_metadata(summ_name)
            summary_txt = self.storage.load_text_blob(summ_name)
            summary = json.loads(summary_txt)

            brief_txt = self.template_brief(memo_name)

            record_metadata = metadata.copy() | dict(blob_path = diff_name, timestamp = int(time.time()))
            record_metadata = {k:v for k,v in record_metadata.items() if k in self.metadata_fields}

            records.append(rg.Record(
                fields={
                    "diff_html": diff,
                    "memo_output": brief_txt,
                    "summary_output": summary_txt,
                },
                suggestions=[  # Pre-fill from model
                    rg.Suggestion("practically_substantive", value=str(summary['practically_substantive']['rating'])),
                ],
                metadata= record_metadata,
            ))
            if len(records) == max_examples:
                dataset.records.log(records)
                return

        if not records:
            logger.info("No records added.")


    def template_brief(self, brief_name):
        brief_obj = self.storage.load_json_blob(brief_name)
        parts = ["Relevant:",
                 str(brief_obj["relevance_flag"]),
                "Section Memo:",
                 brief_obj["section_memo"],
                 "Running Memo:",
                 brief_obj["running_memo"]]
        return "\n".join(parts)


    def template_diffs(self, diff_name) -> dict:
        diff_obj = self.storage.load_json_blob(diff_name)
        # xxx: argilla does NOT accept list values.
        # so must be a string-keyed dict
        diffs : dict[str, dict[str, str]] = {}
        for d in diff_obj['diffs']:
            before, after = self.render_span_as_html(d['tag'], d['before'], d['after'])
            key = str(d['idx'])
            diffs.setdefault(key, {"before": "", "after": ""})
            diffs[key]['before'] += before
            diffs[key]['after'] += after
        return diffs


    @staticmethod
    def render_span_as_html(tag: str, a: str, b: str) -> tuple[str, str]:
            """
            Returns two strings with span-level diff highlighting.
            """
            if tag == 'equal':
                return a, b
            elif tag == 'replace':
                return f'<span class="diff_sub">{a}</span>', f'<span class="diff_add">{b}</span>'
            elif tag == 'delete':
                return f'<span class="diff_sub">{a}</span>', ''
            elif tag == 'insert':
                return '', f'<span class="diff_add">{b}</span>'
            else:
                return '', ''

class BriefV2Dataset(BriefV1Dataset):
    def template_brief(self, brief_name):
        brief_obj = self.container.storage.load_json_blob(brief_name)
        # TODO: This shouldn't be so complicated but sometimes Memos are saved as Briefs
        try:
            memo = Memo.model_validate(brief_obj)
            parts = ["Section Memo:",
                     memo.section_memo,
                     "Running Memo:",
                     memo.running_memo]
            return "\n".join(parts)
        except ValidationError as e:
            brief = Brief.model_validate(brief_obj)
            parts = [["Section Memo:", m.section_memo, "Running Memo:", m.running_memo] for m in brief.memos]
            return "\n".join((s for p in parts for s in p))


class BriefLabelBase(BaseModel):
    ...


class BriefLabelV1(BriefLabelBase):
    practically_substantive_true: Optional[bool]
    practically_substantive_pred: bool

    @classmethod
    def from_dict(cls, label: dict):
        pst = label['responses'].get('practically_substantive',[{}])[0].get('value')
        psp = label['suggestions']['practically_substantive']['value']
        pst = optional_bool(pst)
        psp = cast(bool, optional_bool(psp))
        return BriefLabelV1(
            practically_substantive_true = pst,
            practically_substantive_pred = psp
        )


class BriefLabelV2(BriefLabelV1):
    notes_good: Optional[bool]

    @classmethod
    def from_dict(cls, label: dict):
        v1 = super().from_dict(label)
        v2 = cls.migrate(v1)
        good = label['responses'].get('notes_good',[{}])[0].get('value')
        v2.notes_good = optional_bool(good)
        return v2

    @classmethod
    def migrate(cls, v1: BriefLabelV1) -> Self:
        if not isinstance(v1, BriefLabelV1):
            raise TypeError(f"Expected BriefLabelV1, got {type(v1)}")
        v2 = cls(practically_substantive_true = v1.practically_substantive_true,
                 practically_substantive_pred = v1.practically_substantive_pred,
                 notes_good = None)
        return v2


def optional_bool(value: str) -> Optional[bool]:
    return {'True': True, 'False': False, "unsure": None}.get(value, None)
