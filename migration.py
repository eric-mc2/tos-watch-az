import os
import ulid 
import json
import logging
from typing import List, Optional
from dotenv import load_dotenv
from pydantic import ValidationError
from pydantic_xml import BaseXmlModel, attr, element
from src.differ import DiffDoc, DiffSection
from src.stages import Stage
from src.docchunk import DocChunk
from src.differ import _diff_byspan, _get_manifest
from src.blob_utils import (list_blobs_nest, 
                            list_blobs,
                            set_connection_key, 
                            upload_text_blob, 
                            upload_json_blob, 
                            load_text_blob, 
                            load_json_blob,
                            load_metadata,
                            remove_blob,
                            check_blob)


logging.getLogger('azure').setLevel(logging.WARNING)

def migrate_raw():
    blob_names = list_blobs_nest()
    for company, policies in blob_names[Stage.SUMMARY_RAW.value].items():
        for policy, files in policies.items():
            for file in files:
                run_id = ulid.ulid()
                timestamp = file.removesuffix(".txt")
                in_path = f"{Stage.SUMMARY_RAW.value}/{company}/{policy}/{timestamp}.txt"
                out_path = f"{Stage.SUMMARY_RAW.value}/{company}/{policy}/{timestamp}/{run_id}.txt"
                metadata = dict(
                    run_id = run_id,
                    prompt_version = "v1",
                    schema_version = "v1",
                )
                
                txt = load_text_blob(in_path)
                
                out_path = f"{Stage.SUMMARY_RAW.value}/{company}/{policy}/{timestamp}/{run_id}.txt"
                upload_text_blob(txt, out_path, metadata=metadata)
                out_path = f"{Stage.SUMMARY_RAW.value}/{company}/{policy}/{timestamp}/latest.txt"
                upload_text_blob(txt, out_path, metadata=metadata)
                remove_blob(in_path)

def migrate_clean():
    blob_names = list_blobs_nest()
    for company, policies in blob_names[Stage.SUMMARY_CLEAN.value].items():
        for policy, files in policies.items():
            for file in files:
                timestamp = file.removesuffix(".json")
                in_path = f"{Stage.SUMMARY_CLEAN.value}/{company}/{policy}/{timestamp}.json"
                raw_path = f"{Stage.SUMMARY_RAW.value}/{company}/{policy}/{timestamp}/latest.txt"
                
                upstream_metadata = load_metadata(raw_path)

                txt = load_json_blob(in_path)
                
                out_path = f"{Stage.SUMMARY_CLEAN.value}/{company}/{policy}/{timestamp}/{run_id}.json"
                upload_json_blob(json.dumps(txt, indent=2), out_path, metadata=upstream_metadata)
                out_path = f"{Stage.SUMMARY_CLEAN.value}/{company}/{policy}/{timestamp}/latest.json"
                upload_json_blob(json.dumps(txt, indent=2), out_path, metadata=upstream_metadata)
                remove_blob(in_path)

def migrate_diff_dir():
    blobs = list_blobs()
    for blob in blobs:
        if blob.startswith("05-diffs"):
            dest = blob.replace("05-diffs", "05-diffs-raw")
            txt = load_text_blob(blob)
            upload_json_blob(txt, dest)
            remove_blob(blob)


def migrate_diffs():
    class DiffSectionXML(BaseXmlModel):
        index: str = attr(name="idx")
        before: str = element(nillable=True, default="")
        after: str = element(nillable=True, default="")

    class DiffDocXML(BaseXmlModel, tag="diff_sections"):
        diffs: List[DiffSectionXML] = element(tag="section")

    blobs = list_blobs()
    for blob in blobs:
        if blob.startswith("06-prompts"):
            dest = blob.replace("06-prompts", "05-diffs-clean").replace(".txt", ".json")
            txt = load_text_blob(blob)
            try:
                parsed = DiffDocXML.from_xml(txt)
            except ValidationError as e:
                continue
            parsed = DiffDoc(diffs=[DiffSection(index=int(d.index),before=d.before,after=d.after) for d in parsed.diffs])
            upload_json_blob(parsed.model_dump_json(), dest)
            remove_blob(blob)

def migrate_labels():
    blobs = list_blobs()
    for blob in blobs:
        if blob.startswith("09-labels"):
            labels = load_json_blob(blob)
            for label in labels:
                cleaned = "05-diffs-clean/" + label['metadata']['blob_path'].removeprefix("05-diffs/")
                label['metadata']['blob_path'] = cleaned
            upload_json_blob(json.dumps(labels, indent=2), blob)

def backfill_diffs():
    blobs = list_blobs_nest()
    for stage, companies in blobs.items():
        if stage != Stage.DOCCHUNK.value:
            continue
        for company, policies in companies.items():
            for policy in policies.keys():
                manifest = _get_manifest(company, policy)
                for after, before in manifest.items():
                    before = os.path.join(stage, company, policy, before)
                    after = os.path.join(stage, company, policy, after)
                    if not check_blob(before) or not check_blob(after):
                        continue
                    doca = load_json_blob(before)
                    docb = load_json_blob(after)
                    txta = [DocChunk.from_str(x).text for x in doca]
                    txtb = [DocChunk.from_str(x).text for x in docb]
                    span_diff = _diff_byspan(before, after, txta, txtb)
                    sd = json.loads(span_diff)
                    if not all([x['tag'] == "equal" for x in sd['diffs']]):
                        out_name = after.replace(Stage.DOCCHUNK.value, Stage.DIFF_SPAN.value)
                        upload_json_blob(span_diff, out_name)



if __name__ == "__main__":
    load_dotenv()
    set_connection_key("AzureWebJobsStorage")
    # print("Uncomment if you're sure you want to run this again!")
    # migrate_raw()
    # migrate_clean()    
    # migrate_diff_dir()
    # migrate_diffs()
    # migrate_labels()
    # backfill_diffs()
