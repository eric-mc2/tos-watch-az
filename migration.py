import os
from src.blob_utils import (list_blobs_nest, 
                            set_connection_key, 
                            upload_text_blob, 
                            upload_json_blob, 
                            load_text_blob, 
                            load_json_blob,
                            load_metadata,
                            remove_blob)
from src.stages import Stage
from dotenv import load_dotenv
from src.summarizer import SCHEMA_VERSION, PROMPT_VERSION
import ulid 
import json
import logging

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


if __name__ == "__main__":
    load_dotenv()
    set_connection_key("AzureWebJobsStorage")
    # print("Uncomment if you're sure you want to run this again!")
    # migrate_raw()
    # migrate_clean()    