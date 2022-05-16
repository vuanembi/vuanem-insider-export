from typing import Optional
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

SCHEMA = [
    {"name": "a_mob_user_id", "type": "NUMERIC"},
    {"name": "a_mli", "type": "STRING"},
    {"name": "a_email_double_optin", "type": "NUMERIC"},
    {"name": "a_gdpr", "type": "NUMERIC"},
    {"name": "a_email", "type": "STRING"},
    {"name": "a_name", "type": "STRING"},
    {"name": "a_surname", "type": "STRING"},
    {"name": "a_gender", "type": "STRING"},
    {"name": "a_birthday", "type": "STRING"},
    {"name": "a_age", "type": "NUMERIC"},
    {"name": "a_language", "type": "STRING"},
    {"name": "a_city", "type": "STRING"},
    {"name": "a_app_push_engage", "type": "NUMERIC"},
    {"name": "a_country", "type": "STRING"},
    {"name": "a_phone_number", "type": "STRING"},
    {"name": "a_whatsapp_optin", "type": "NUMERIC"},
    {"name": "a_sms_optin", "type": "NUMERIC"},
    {"name": "a_unique_user_id", "type": "STRING"},
    {"name": "a_last_purchase_amount", "type": "NUMERIC"},
    {"name": "a_last_purchased_product_name", "type": "STRING"},
    {"name": "a_last_abandoned_cart_amount", "type": "NUMERIC"},
    {"name": "a_last_visited_product_name", "type": "STRING"},
    {"name": "a_last_visited_category", "type": "STRING"},
    {"name": "a_list_id", "type": "STRING"},
    {"name": "a_search_query", "type": "STRING"},
    {"name": "a_cart_abandoned", "type": "NUMERIC"},
    {"name": "a_last_visited_product_img", "type": "STRING"},
    {"name": "a_last_visited_product_url", "type": "STRING"},
    {"name": "a_last_visited_category_url", "type": "STRING"},
    {"name": "a_device_type", "type": "STRING"},
    {"name": "a_last_abandoned_product_img", "type": "STRING"},
    {"name": "a_last_abandoned_product_url", "type": "STRING"},
    {"name": "a_last_abandoned_product_name", "type": "STRING"},
    {"name": "a_last_email_open_date", "type": "STRING"},
    {"name": "a_bounce", "type": "NUMERIC"},
    {"name": "a_last_visited_product_id", "type": "STRING"},
    {"name": "a_last_purchase_date", "type": "TIMESTAMP"},
    {"name": "a_last_visit_date", "type": "TIMESTAMP"},
    {"name": "a_email_sha256", "type": "STRING"},
    {"name": "a_custom_segment_id", "type": "STRING"},
    {"name": "a_identified_date", "type": "TIMESTAMP"},
    {"name": "a_c_url_chitest", "type": "STRING"},
    {"name": "a_web_push_optin", "type": "NUMERIC"},
    {"name": "a_sr_user_based", "type": "STRING"},
    {"name": "a_recommendation_ub_webpush", "type": "STRING"},
    {"name": "a_global_unsubscribe", "type": "NUMERIC"},
    {"name": "a_c_submit_lead", "type": "NUMERIC"},
    {"name": "a_c_user_name", "type": "STRING"},
    {"name": "a_c_user_phone", "type": "STRING"},
    {"name": "a_c_lead_coupon_code", "type": "STRING"},
    {"name": "a_c_lead_coupon_type", "type": "STRING"},
    {"name": "a_contact_source", "type": "STRING"},
    {"name": "a_active_journeys", "type": "STRING"},
    {"name": "a_c_phone_number", "type": "STRING"},
    {"name": "a_spam", "type": "NUMERIC"},
    {"name": "a_has_invalid_email", "type": "NUMERIC"},
    {"name": "a_blocked", "type": "NUMERIC"},
    {"name": "a_email_optin", "type": "NUMERIC"},
    {"name": "a_partner", "type": "STRING"},
    {"name": "a_source", "type": "STRING"},
    {"name": "a_active", "type": "NUMERIC"},
    {"name": "a_created_date", "type": "TIMESTAMP"},
    {"name": "a_updated_date", "type": "TIMESTAMP"},
    {"name": "event_name", "type": "STRING"},
    {"name": "e_c_url_pagelocation", "type": "STRING"},
    {"name": "e_product_id", "type": "STRING"},
    {"name": "e_timestamp", "type": "TIMESTAMP"},
    {"name": "e_session_id", "type": "STRING"},
    {"name": "e_currency", "type": "STRING"},
    {"name": "e_device_type", "type": "STRING"},
    {"name": "e_event_group_id", "type": "STRING"},
    {"name": "e_name", "type": "STRING"},
    {"name": "e_product_image_url", "type": "STRING"},
    {"name": "e_quantity", "type": "NUMERIC"},
    {"name": "e_referrer", "type": "STRING"},
    {"name": "e_source", "type": "STRING"},
    {"name": "e_unit_price", "type": "NUMERIC"},
    {"name": "e_url", "type": "STRING"},
    {"name": "e_unit_sale_price", "type": "NUMERIC"},
    {"name": "e_c_type", "type": "STRING"},
    {"name": "e_status", "type": "STRING"},
    {"name": "e_c_landingpage_url", "type": "STRING"},
    {"name": "e_c_source_url", "type": "STRING"},
    {"name": "e_campaign_id", "type": "NUMERIC"},
    {"name": "e_c_ins_voucher_end_date", "type": "STRING"},
    {"name": "e_is_dry_run", "type": "NUMERIC"},
    {"name": "e_journey_id", "type": "NUMERIC"},
    {"name": "e_reason", "type": "STRING"},
    {"name": "e_action_code", "type": "NUMERIC"},
    {"name": "e_channel_code", "type": "STRING"},
    {"name": "e_taxonomy", "type": "STRING"},
    {"name": "e_search_query", "type": "STRING"},
    {"name": "e_product_variant_id", "type": "STRING"},
    {"name": "e_promotion_name", "type": "STRING"},
    {"name": "iid", "type": "STRING"},
]


def get_file_uri(input_: str) -> str:
    name = parse.urlparse(input_).path.split("/").pop()
    return f"gs://{BUCKET}/user-data-exports/{name}"


def stream_file(input_: str):
    filename = get_file_uri(input_)
    with requests.get(input_, stream=True) as r, GcsIO().open(filename, "wb") as f:
        shutil.copyfileobj(r.raw, f)
    return [filename]


def transform(element):
    def _parse_ts(value: str) -> Optional[str]:
        return value.replace("Z", "") if value else None

    return {
        **element,
        **{
            k: _parse_ts(element[k])
            for k in [i["name"] for i in SCHEMA if i["type"] == "TIMESTAMP"]
        },
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
            | "Transform" >> beam.Map(transform)
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                TABLE,
                DATASET,
                schema={"fields": SCHEMA},
                additional_bq_parameters={
                    "timePartitioning": {"field": "e_timestamp", "type": "DAY"},
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
