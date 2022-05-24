import dayjs, { Dayjs } from 'dayjs';
import utc from 'dayjs/plugin/utc';

import * as repo from './repo';
import { launchJob } from '../dataflow';

dayjs.extend(utc);

export const callbackRoute = 'callback'
const callbackHook = `${process.env.PUBLIC_URL}/${callbackRoute}`;

export const template = process.env.TEMPLATE || '';

const buildConfig = (start: Dayjs, end: Dayjs) => ({
    vertical: {
        sources: ['last'],
        main_group: {
            condition: 'and',
            filter_groups: [
                {
                    condition: 'and',
                    attr_filters: [
                        {
                            attrs: [
                                {
                                    key: 'iid',
                                    operator: 'ne',
                                    values: ['any'],
                                },
                            ],
                        },
                    ],
                },
            ],
        },
    },
    attributes: ['*'],
    events: {
        start_date: start.unix(),
        end_date: end.unix(),
        wanted: [
            {
                event_name: 'c2m',
                params: ['c_url_pagelocation', 'pid', 'timestamp'],
            },
            {
                event_name: 'cart_cleared',
                params: ['sid', 'timestamp'],
            },
            {
                event_name: 'cart_page_view',
                params: [
                    'cu',
                    'device_type',
                    'e_guid',
                    'na',
                    'pid',
                    'piu',
                    'qu',
                    'referrer',
                    'sid',
                    'source',
                    'up',
                    'url',
                    'usp',
                    'timestamp',
                ],
            },
            {
                event_name: 'cdndataz',
                params: ['c_type', 'timestamp'],
            },
            {
                event_name: 'confirmation_page_view',
                params: [
                    'cu',
                    'device_type',
                    'e_guid',
                    'na',
                    'pid',
                    'piu',
                    'qu',
                    'referrer',
                    'sid',
                    'source',
                    'up',
                    'url',
                    'usp',
                    'timestamp',
                ],
            },
            {
                event_name: 'geofence_trigger',
                params: ['name', 'status', 'timestamp'],
            },
            {
                event_name: 'homepage_view',
                params: [
                    'device_type',
                    'referrer',
                    'sid',
                    'source',
                    'url',
                    'timestamp',
                ],
            },
            {
                event_name: 'ins_address_fill',
                params: ['na', 'timestamp'],
            },
            {
                event_name: 'ins_call_button_click',
                params: ['timestamp'],
            },
            {
                event_name: 'ins_full_name_fill',
                params: ['na', 'timestamp'],
            },
            {
                event_name: 'ins_lead_submitted',
                params: [
                    'c_landingpage_url',
                    'c_source_url',
                    'campaign_id',
                    'url',
                    'timestamp',
                ],
            },
            {
                event_name: 'ins_mess_button_click',
                params: ['timestamp'],
            },
            {
                event_name: 'ins_phone_number_fill',
                params: ['na', 'timestamp'],
            },
            {
                event_name: 'ins_voucher_page_visit',
                params: [
                    'c_ins_voucher_end_date',
                    'campaign_id',
                    'na',
                    'timestamp',
                ],
            },
            {
                event_name: 'item_added_to_cart',
                params: [
                    'cu',
                    'na',
                    'pid',
                    'piu',
                    'qu',
                    'sid',
                    'up',
                    'url',
                    'usp',
                    'timestamp',
                ],
            },
            {
                event_name: 'item_removed_from_cart',
                params: [
                    'cu',
                    'na',
                    'pid',
                    'piu',
                    'qu',
                    'sid',
                    'up',
                    'url',
                    'usp',
                    'timestamp',
                ],
            },
            {
                event_name: 'journey_enter',
                params: [
                    'is_dry_run',
                    'journey_id',
                    'name',
                    'reason',
                    'timestamp',
                ],
            },
            {
                event_name: 'journey_exited',
                params: ['is_dry_run', 'journey_id', 'name', 'timestamp'],
            },
            {
                event_name: 'journey_product_action',
                params: [
                    'action_code',
                    'campaign_id',
                    'channel_code',
                    'journey_id',
                    'timestamp',
                ],
            },
            {
                event_name: 'lead_collected',
                params: ['campaign_id', 'timestamp'],
            },
            {
                event_name: 'listing_page_view',
                params: [
                    'device_type',
                    'referrer',
                    'sid',
                    'source',
                    'ta',
                    'url',
                    'timestamp',
                ],
            },
            {
                event_name: 'other_page_view',
                params: [
                    'device_type',
                    'referrer',
                    'search_query',
                    'sid',
                    'source',
                    'url',
                    'timestamp',
                ],
            },
            {
                event_name: 'product_detail_page_view',
                params: [
                    'cu',
                    'device_type',
                    'na',
                    'pid',
                    'piu',
                    'product_variant_id',
                    'qu',
                    'referrer',
                    'sid',
                    'source',
                    'ta',
                    'up',
                    'url',
                    'usp',
                    'timestamp',
                ],
            },
            {
                event_name: 'product_page_view_detail',
                params: [
                    'c_landingpage_url',
                    'c_source_url',
                    'pid',
                    'timestamp',
                ],
            },
            {
                event_name: 'web-visit',
                params: ['pn', 'timestamp'],
            },
            {
                event_name: 'web_visit',
                params: ['c_type', 'timestamp'],
            },
        ],
    },
    format: 'parquet',
    hook: callbackHook,
});

export const requestExport = async (start?: string, end?: string) => {
    const [_start, _end] = (
        [
            [1, start],
            [7, end],
        ] as [number, string?][]
    ).map(
        ([days, input_]) =>
            (input_ && dayjs(input_)) || dayjs().subtract(days, 'day'),
    );

    return repo
        .requestExport(buildConfig(_start, _end))
        .then((status) => ({ status }));
};

export const exportPipeline = (url: string) =>
    launchJob(template, { input: url }, 'insider-export').then((id) => ({
        id,
    }));
