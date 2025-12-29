import argilla as rg
import os
from src.blob_utils import list_blobs_nest, load_text_blob, set_connection_key, load_json_blob, load_metadata
from src.stages import Stage
import time
import json
import difflib
from dotenv import load_dotenv
import argparse

def create_dataset(client: rg.Argilla, name):
    dataset = client.datasets(name)
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


def create_records(dataset, schema_version, prompt_version, max_examples=10):
    # Stream from blob storage
    blob_names = list_blobs_nest()
    
    records = []
    for company, policies in blob_names[Stage.SUMMARY_CLEAN.value].items():
        for policy, timestamps in policies.items():
            for timestamp, files in timestamps.items():
                for file in files:
                    # Only add records for specified versions
                    summ_name = os.path.join(Stage.SUMMARY_CLEAN.value, company, policy, timestamp, file)
                    metadata = load_metadata(summ_name)
                    if metadata['schema_version'] != schema_version or metadata['prompt_version'] != prompt_version:
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
                    diff = template_diffs(diff_name)
                    if not diff:
                        continue

                    
                    summary_txt = load_text_blob(summ_name)
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
        print("No records added.")


def template_diffs(diff_name) -> dict:
    diff_obj = load_json_blob(diff_name)
    # xxx: argilla does NOT accept list values.
    # so must be a string-keyed dict
    diffs = {}
    for i,d in enumerate(diff_obj['diffs']):
        if d['tag'] != 'equal':
            before = '\n'.join(d['before'])
            after = '\n'.join(d['after'])
            before, after = render_diff_as_html(before, after)
            diffs[str(i)] = {"before": before, "after": after}
    return diffs

def render_diff_as_html(before: str, after: str) -> tuple[str, str]:
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


def get_data(client: rg.Argilla, name: str):
    # TODO: Might as well automatically push the dataset to blob storage 
    dataset = client.datasets(name)
    if dataset is None:
        print(f"Dataset {name} does not exist yet!")
        return
    data_dir = f"data/{dataset_version}"
    if os.path.exists(data_dir):
        print("Dataset already downloaded. Archive or delete it. Then re-run.")
        return
    os.makedirs(data_dir)
    dataset.to_disk(data_dir)


if __name__ == "__main__":
    load_dotenv()

    client = rg.Argilla(
        api_url="https://eric-mc22-tos-watch-ft.hf.space",
        api_key=os.environ['ARGILLA_API_KEY'],
        headers={"Authorization": f"Bearer {os.environ['HF_TOKEN']}"}
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["add", "download"], required=True)
    args = parser.parse_args()

    dataset_version = "substantive_v1"
    schema_version = "v1"
    prompt_version = "v1"
    if args.action == "add":
        set_connection_key("AzureWebJobsStorage")
        dataset = create_dataset(client, dataset_version)
        create_records(dataset, schema_version, prompt_version, 20)
    if args.action == "download":
        get_data(client, dataset_version)
    