import argparse
import logging
from scripts.labeling.brief_v1 import BriefV1Dataset
from scripts.labeling.dataset import DatasetBase
from scripts.labeling.summary_v1 import SummaryV1Dataset


from src.utils.app_utils import load_env_vars

# TODO: Next here. Need to label 10 examples per task. Honestly just run the argilla server and tell
# gemini to create 4 personas with different note-taking styles and then have each persona evaluate the doc.
# And then use the answer you like.
# If ICL alone doesn't work, then ask gemini to refine the system prompts. 

logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('argilla').setLevel(logging.INFO)


if __name__ == "__main__":
    load_env_vars()

    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["add", "download"], required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--prompt_version", required=True)
    parser.add_argument("--schema_version", required=True)
    args = parser.parse_args()

    dataset_name = f"{args.workflow}_{args.prompt_version}"
    makers = {"summary_v1": SummaryV1Dataset, "brief_v1": BriefV1Dataset}
    if args.action == "add":
        if dataset_name not in makers:
            exit(1)
        maker = makers[dataset_name]()
        dataset = maker.create_dataset(dataset_name)
        maker.create_records(dataset, args.schema_version, args.prompt_version, 20)
    elif args.action == "download":
        maker = DatasetBase()
        maker.get_data(dataset_name)