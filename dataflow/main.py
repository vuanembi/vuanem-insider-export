from typing import Optional
import argparse
import os
import csv
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "dataflow/voltaic-country-280607-ea3eb5348029.json"

SCHEMA = [
    {"name": "a_mob_user_id", "type": "STRING"},
    {"name": "a_mli", "type": "STRING"},
    {"name": "a_email_double_optin", "type": "STRING"},
    {"name": "a_gdpr", "type": "STRING"},
    {"name": "a_email", "type": "STRING"},
    {"name": "a_name", "type": "STRING"},
    {"name": "a_surname", "type": "STRING"},
    {"name": "a_gender", "type": "STRING"},
    {"name": "a_birthday", "type": "STRING"},
    {"name": "a_age", "type": "STRING"},
    {"name": "a_language", "type": "STRING"},
    {"name": "a_city", "type": "STRING"},
    {"name": "a_app_push_engage", "type": "STRING"},
    {"name": "a_country", "type": "STRING"},
    {"name": "a_phone_number", "type": "STRING"},
    {"name": "a_whatsapp_optin", "type": "STRING"},
    {"name": "a_sms_optin", "type": "STRING"},
    {"name": "a_unique_user_id", "type": "STRING"},
    {"name": "a_last_purchase_amount", "type": "STRING"},
    {"name": "a_last_purchased_product_name", "type": "STRING"},
    {"name": "a_last_abandoned_cart_amount", "type": "STRING"},
    {"name": "a_last_visited_product_name", "type": "STRING"},
    {"name": "a_last_visited_category", "type": "STRING"},
    {"name": "a_list_id", "type": "STRING"},
    {"name": "a_search_query", "type": "STRING"},
    {"name": "a_cart_abandoned", "type": "STRING"},
    {"name": "a_last_visited_product_img", "type": "STRING"},
    {"name": "a_last_visited_product_url", "type": "STRING"},
    {"name": "a_last_visited_category_url", "type": "STRING"},
    {"name": "a_device_type", "type": "STRING"},
    {"name": "a_last_abandoned_product_img", "type": "STRING"},
    {"name": "a_last_abandoned_product_url", "type": "STRING"},
    {"name": "a_last_abandoned_product_name", "type": "STRING"},
    {"name": "a_last_email_open_date", "type": "STRING"},
    {"name": "a_bounce", "type": "STRING"},
    {"name": "a_last_visited_product_id", "type": "STRING"},
    {"name": "a_last_purchase_date", "type": "TIMESTAMP"},
    {"name": "a_last_visit_date", "type": "TIMESTAMP"},
    {"name": "a_email_sha256", "type": "STRING"},
    {"name": "a_custom_segment_id", "type": "STRING"},
    {"name": "a_identified_date", "type": "TIMESTAMP"},
    {"name": "a_c_url_chitest", "type": "STRING"},
    {"name": "a_web_push_optin", "type": "STRING"},
    {"name": "a_sr_user_based", "type": "STRING"},
    {"name": "a_recommendation_ub_webpush", "type": "STRING"},
    {"name": "a_global_unsubscribe", "type": "STRING"},
    {"name": "a_c_submit_lead", "type": "STRING"},
    {"name": "a_c_user_name", "type": "STRING"},
    {"name": "a_c_user_phone", "type": "STRING"},
    {"name": "a_c_lead_coupon_code", "type": "STRING"},
    {"name": "a_c_lead_coupon_type", "type": "STRING"},
    {"name": "a_contact_source", "type": "STRING"},
    {"name": "a_active_journeys", "type": "STRING"},
    {"name": "a_c_phone_number", "type": "STRING"},
    {"name": "a_spam", "type": "STRING"},
    {"name": "a_has_invalid_email", "type": "STRING"},
    {"name": "a_blocked", "type": "STRING"},
    {"name": "a_email_optin", "type": "STRING"},
    {"name": "a_partner", "type": "STRING"},
    {"name": "a_source", "type": "STRING"},
    {"name": "a_active", "type": "STRING"},
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
    {"name": "e_quantity", "type": "STRING"},
    {"name": "e_referrer", "type": "STRING"},
    {"name": "e_source", "type": "STRING"},
    {"name": "e_unit_price", "type": "STRING"},
    {"name": "e_url", "type": "STRING"},
    {"name": "e_unit_sale_price", "type": "STRING"},
    {"name": "e_c_type", "type": "STRING"},
    {"name": "e_status", "type": "STRING"},
    {"name": "e_c_landingpage_url", "type": "STRING"},
    {"name": "e_c_source_url", "type": "STRING"},
    {"name": "e_campaign_id", "type": "STRING"},
    {"name": "e_c_ins_voucher_end_date", "type": "STRING"},
    {"name": "e_is_dry_run", "type": "STRING"},
    {"name": "e_journey_id", "type": "STRING"},
    {"name": "e_reason", "type": "STRING"},
    {"name": "e_action_code", "type": "STRING"},
    {"name": "e_channel_code", "type": "STRING"},
    {"name": "e_taxonomy", "type": "STRING"},
    {"name": "e_search_query", "type": "STRING"},
    {"name": "e_product_variant_id", "type": "STRING"},
    {"name": "e_promotion_name", "type": "STRING"},
    {"name": "iid", "type": "STRING"},
]


def parse_line(element):
    reader = csv.DictReader([element], fieldnames=[i["name"] for i in SCHEMA])
    return list(reader)[0]


def clean_nulls(element):
    return {k: v if v != "\\N" else None for k, v in element.items()}


def transform_timestamp(element):
    def _parse(value: str) -> Optional[str]:
        return value.replace("Z", "") if value else None

    return {
        **element,
        **{k: _parse(element[k]) for k in [i["name"] for i in SCHEMA]},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default="gs://vuanem-insider/user-data-exports2/2022-05-13T04:35:33.405013445Z.csv",
    )
    parser.add_argument(
        "--output",
        # default="gs://vuanem-insider/user-data-exports2/123.json",
        default="123.json",
    )
    args = parser.parse_args()

    options = PipelineOptions(
        runner="DataFlowRunner",
        temp_location="gs://vuanem-insider/temp",
        project="voltaic-country-280607",
        region="us-central1",
    )
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadCSV" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "ToDicts" >> beam.Map(parse_line)
            | "CleanNulls" >> beam.Map(clean_nulls)
            | "TransformTimestamp" >> beam.Map(transform_timestamp)
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                "p_UserDataExport",
                "IP_Insider",
                schema={"fields": SCHEMA},
                additional_bq_parameters={
                    "timePartitioning": {"field": "e_timestamp", "type": "DAY"},
                },
            )
            # | "Dumps" >> beam.Map(json.dumps)
            # | "Write" >> beam.io.WriteToText(args.output, num_shards=1)
        )


if __name__ == "__main__":
    import os

    main()
