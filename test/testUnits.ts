import { createFileName } from '../src/storage/cloudStorage';
import {
    streamExportService,
    loadService,
    extractFileName,
} from '../src/insider/service';

jest.setTimeout(5_000_000);

const url =
    'https://insider-data-export.useinsider.com/vuanem/p/2022-05-13T04%3A35%3A33.405013445Z.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA5T7ZNA5RBXF4GLW4%2F20220513%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Date=20220513T043600Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=2d407d914693010c3360dc09a7f820c4570cf2cfb862b9f662475defc46d5996';

const uri = createFileName(<string>extractFileName(url));

it('Stream', () =>
    streamExportService(url).then((filename) => expect(filename).toEqual(uri)));

it('Load', () => loadService(uri).then((res) => expect(res).toBeTruthy()));
