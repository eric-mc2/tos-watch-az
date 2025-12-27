import os
import ulid 
import json
import logging
from typing import List, Optional
from dotenv import load_dotenv
from pydantic import ValidationError
from pydantic_xml import BaseXmlModel, attr, element
from src.differ import DiffDoc, DiffSection
from src.summarizer import SCHEMA_VERSION, PROMPT_VERSION
from src.stages import Stage
from src.blob_utils import (list_blobs_nest, 
                            list_blobs,
                            set_connection_key, 
                            upload_text_blob, 
                            upload_json_blob, 
                            load_text_blob, 
                            load_json_blob,
                            load_metadata,
                            remove_blob)

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
                    prompt_version = PROMPT_VERSION,
                    schema_version = SCHEMA_VERSION,
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




if __name__ == "__main__":
    load_dotenv()
    set_connection_key("AzureWebJobsStorage")
    # print("Uncomment if you're sure you want to run this again!")
    # migrate_raw()
    # migrate_clean()    
    # migrate_diff_dir()
    # migrate_diffs()
    migrate_labels()
