from typing import Any, Iterable
import argparse
import logging
from urllib import parse
import shutil

import requests

import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.options.pipeline_options import PipelineOptions
from google.auth import default

BUCKET = "vuanem-insider"

DATASET = "IP_Insider"

TABLE = "p_UserDataExport"

TIMESTAMP_FIELDS = [
    "a_last_purchase_date",
    "a_last_visit_date",
    "a_identified_date",
    "a_created_date",
    "a_updated_date",
    "e_timestamp",
]


SCHEMA = [
    {"name": "iid", "type": "STRING"},
    {"name": "e_session_id", "type": "STRING"},
    {"name": "event_name", "type": "STRING"},
    {"name": "e_timestamp", "type": "TIMESTAMP"},
    {
        "name": "attributes",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "key", "type": "STRING"},
            {"name": "value", "type": "STRING"},
        ],
    },
    {
        "name": "params",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "key", "type": "STRING"},
            {"name": "value", "type": "STRING"},
        ],
    },
]


def get_file_uri(input_: str) -> str:
    name = parse.urlparse(input_).path.split("/").pop()
    return f"gs://{BUCKET}/user-data-exports/{name}"


def stream_file(input_: str) -> Iterable[str]:
    filename = get_file_uri(input_)
    with requests.get(input_, stream=True) as r, GcsIO().open(filename, "wb") as f:
        shutil.copyfileobj(r.raw, f)
    return [filename]


def transform_type(element: dict[str, Any]) -> dict[str, Any]:
    return {
        **element,
        **{
            k: (lambda x: x.replace("Z", "") if x else None)(element[k])
            for k in TIMESTAMP_FIELDS
        },
    }


def transform_nulls(element: dict[str, Any]) -> dict[str, Any]:
    return {k: (lambda x: None if x == "" else x)(v) for k, v in element.items()}


def transform_eav(element: dict[str, Any]) -> dict[str, Any]:
    def _encode_eav(initial: str) -> list[dict[str, Any]]:
        return [
            {"key": k, "value": str(v)}
            for k, v in element.items()
            if k.startswith(initial)
        ]

    return {
        **{
            k: v
            for k, v in element.items()
            if k in [i["name"] for i in SCHEMA if i["type"] != "RECORD"]
        },
        **{"attributes": _encode_eav("a_")},
        **{"params": _encode_eav("e_")},
    }


def main(args: argparse.Namespace, beam_args: list[str]):
    options = PipelineOptions(
        beam_args,
        runner=args.runner,
        project=args.project,
        temp_location=args.temp_location,
        region=args.region,
        save_main_session=True,
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | "InitializeURL" >> beam.Create([args.input])
            | "StreamFile" >> beam.ParDo(stream_file)
            | "ReadFile" >> beam.io.ReadAllFromParquet()
            | "TransformType" >> beam.Map(transform_type)
            | "TransformNulls" >> beam.Map(transform_nulls)
            | "TransformEAV" >> beam.Map(transform_eav)
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                TABLE,
                DATASET,
                schema={"fields": SCHEMA},
                additional_bq_parameters={
                    "timePartitioning": {"field": "e_timestamp", "type": "DAY"},
                    "clustering": {"fields": ["event_name"]},
                },
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", type=str)
    parser.add_argument("--runner", type=str, default="DataFlowRunner")
    parser.add_argument("--project", type=str, default=default()[1])
    parser.add_argument("--temp_location", type=str, default="gs://vuanem-insider/temp")
    parser.add_argument("--region", type=str, default="us-central1")

    args, beam_args = parser.parse_known_args()

    main(args, beam_args)
