import { Storage } from '@google-cloud/storage';

const client = new Storage();

const bucket = client.bucket('vuanem-insider');

export const createFileName = (filename: string) =>
    `user-data-exports/${filename}`;

export const createFile = (filename: string) => bucket.file(filename);
