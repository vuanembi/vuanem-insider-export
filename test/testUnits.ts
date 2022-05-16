import { requestExport, exportPipeline } from '../src/insider/service';

jest.setTimeout(5_000_000);

const url =
    'https://storage.googleapis.com/vuanem-insider/user-data-exports/123.csv';

it('Request', () =>
    requestExport().then(({ status }) => {
        expect(status).toEqual(200);
    }));
it('Pipeline', () =>
    exportPipeline(url).then(({ id }) => {
        expect(id).toBeTruthy();
    }));
