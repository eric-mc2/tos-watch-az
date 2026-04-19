import argparse
import logging
from scripts.labeling.brief_labels import BriefV1Dataset, BriefV2Dataset
from scripts.labeling.dataset import DatasetBase
from scripts.labeling.summary_labels import SummaryV1Dataset
from src.container import ServiceContainer

from src.utils.app_utils import load_env_vars
from src.utils.log_utils import silence_loggers

silence_loggers()


if __name__ == "__main__":
    load_env_vars()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action")
    add_parser = subparsers.add_parser('add')

    add_parser.add_argument("--workflow", required=True)
    add_parser.add_argument("--prompt_version", required=True)
    add_parser.add_argument("--schema_version", required=True)
    add_parser.add_argument("--n", required=True, type=int)

    download_parser = subparsers.add_parser('download')
    download_parser.add_argument("--workflow", required=True)
    download_parser.add_argument("--prompt_version", required=True)

    push_parser = subparsers.add_parser('push')
    push_parser.add_argument("--workflow", required=True)
    push_parser.add_argument("--prompt_version", required=True)
    args = parser.parse_args()

    dataset_name = f"{args.workflow}_{args.prompt_version}"
    makers = {"summary_v1": SummaryV1Dataset,
              "brief_v1": BriefV1Dataset,
              "brief_v2": BriefV2Dataset}

    container = ServiceContainer.create_real()

    if args.action == "add":
        if dataset_name not in makers:
            exit("Dataset maker does not exist")
        maker = makers[dataset_name](container.storage)
        dataset = maker.create_dataset(dataset_name)
        maker.create_records(dataset, args.schema_version, args.prompt_version, args.n)
    elif args.action == "download":
        maker = DatasetBase(container.storage)
        maker.get_data(dataset_name)
    elif args.action == "push":
        if dataset_name not in makers:
            exit(1)
        maker = makers[dataset_name](container.storage)
        maker.push_data(dataset_name)
