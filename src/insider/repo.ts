import axios from 'axios';
import { ReadStream } from 'fs-extra';

export type ExportConfig = {
    [key: string]: any;
};

export const requestExport = (data: ExportConfig) =>
    axios
        .post('https://unification.useinsider.com/api/raw/v1/export', data)
        .then(({ status }): number => status)
        .catch((err): number => err.isAxiosError && err.response.status);

export const streamExport = (url: string) =>
    axios
        .get<ReadStream>(url, { responseType: 'stream' })
        .then(({ data }) => data);
