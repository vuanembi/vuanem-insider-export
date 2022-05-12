import { createFileName } from '../src/storage/cloudStorage';
import { streamExportService } from '../src/insider/service';

jest.setTimeout(5_000_000);

describe('Stream Service', () => {
    const url =
        'https://www.dundeecity.gov.uk/sites/default/files/publications/civic_renewal_forms.zip';

    it('Stream', () =>
        streamExportService(url).then((filename) =>
            expect(filename).toEqual(createFileName('civic_renewal_forms.zip')),
        ));
});
