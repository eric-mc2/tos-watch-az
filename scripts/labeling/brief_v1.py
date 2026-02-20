import difflib
import json
import os
import time
import argilla as rg
import logging
from random import shuffle

from scripts.labeling.dataset import DatasetBase
from src.stages import Stage
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.INFO)


class BriefV1Dataset(DatasetBase):
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
            ],
            metadata=[
                rg.TermsMetadataProperty(name="model_version"),
                rg.TermsMetadataProperty(name="prompt_version"),
                rg.TermsMetadataProperty(name="schema_version"),
                rg.TermsMetadataProperty(name="blob_path"),
                rg.IntegerMetadataProperty(name="timestamp"),
            ]
        )

        dataset = rg.Dataset(name=name, settings=settings)
        dataset.create()

        return dataset


    def create_records(self, dataset, schema_version, prompt_version, max_examples=10):
        blob_names = self.container.storage.adapter.list_blobs()
        shuffle(blob_names)

        records = []
        for blob_name in blob_names:
            parts = self.container.storage.parse_blob_path(blob_name)

            if parts.stage != Stage.BRIEF_CLEAN.value:
                continue

            # Only add records for specified versions
            memo_name = blob_name
            metadata = self.container.storage.adapter.load_metadata(memo_name)
            if metadata['schema_version'] != schema_version or metadata['prompt_version'] != prompt_version:
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

            summary_txt = self.container.storage.load_text_blob(summ_name)
            summary = json.loads(summary_txt)

            brief_txt = self.template_brief(memo_name)

            records.append(rg.Record(
                fields={
                    "diff_html": diff,
                    "memo_output": brief_txt,
                    "summary_output": summary_txt,
                },
                suggestions=[  # Pre-fill from model
                    rg.Suggestion("practically_substantive", value=str(summary['practically_substantive']['rating'])),
                ],
                metadata={
                    "model_version": "claude-3-5-haiku-20241022",
                    "prompt_version": prompt_version,
                    "schema_version": schema_version,
                    "blob_path": diff_name,
                    "timestamp": int(time.time()),
                }
            ))
            if len(records) == max_examples:
                dataset.records.log(records)
                return

        if not records:
            logger.info("No records added.")


    def template_brief(self, brief_name):
        brief_obj = self.container.storage.load_json_blob(brief_name)
        parts = ["Relevant:",
                 str(brief_obj["relevance_flag"]),
                "Section Memo:",
                 brief_obj["section_memo"],
                 "Running Memo:",
                 brief_obj["running_memo"]]
        return "\n".join(parts)


    def template_diffs(self, diff_name) -> dict:
        diff_obj = self.container.storage.load_json_blob(diff_name)
        # xxx: argilla does NOT accept list values.
        # so must be a string-keyed dict
        diffs = {}
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
