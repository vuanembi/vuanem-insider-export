import { BigQuery } from '@google-cloud/bigquery';
import { File } from '@google-cloud/storage';

type LoadOptions = {
    table: string;
    schema: any;
};

const dataset = 'IP_Insider';

const client = new BigQuery();

const loadStaging = ({ table, schema }: LoadOptions, uri: File) => {
    const _table = `s_${table}`;
    client
        .dataset(dataset)
        .table(_table)
        .load(uri, {
            schema: {
                fields: schema,
            },
            sourceFormat: 'CSV',
        })
        .then(() => _table);
};

export default loadStaging;
