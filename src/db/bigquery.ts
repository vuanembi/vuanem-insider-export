import { BigQuery, TableSchema } from '@google-cloud/bigquery';
import { File } from '@google-cloud/storage';

type LoadOptions = {
    table: string;
    schema: TableSchema;
    partitionKey: string;
};

const dataset = 'IP_Insider';

const client = new BigQuery();

const load = ({ table, schema, partitionKey }: LoadOptions, uri: File) =>
    client
        .dataset(dataset)
        .table(table)
        .load(uri, {
            schema,
            sourceFormat: 'CSV',
            timePartitioning: {
                field: partitionKey,
                type: 'DAY',
            },
        })
        .then(([job]) => job.status)
        .catch((err) => {
            console.log(err);
            return;
        });

export default load;
