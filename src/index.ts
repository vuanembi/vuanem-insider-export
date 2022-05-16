import { HttpFunction } from '@google-cloud/functions-framework';

import {
    requestExport,
    exportPipeline,
    callbackRoute,
} from '../src/insider/service';

export const main: HttpFunction = (req, res) => {
    const { path, body } = req;

    console.log({ path, body });

    const handleErr = (err: any) => {
        console.log(err);
        res.status(500).send(err);
    };

    if (path === `/${callbackRoute}`) {
        exportPipeline(body.url)
            .then((response) => {
                res.send(response);
            })
            .catch(handleErr);
    } else if (path === '/') {
        requestExport(body.start, body.end)
            .then((response) => {
                res.send(response);
            })
            .catch(handleErr);
    } else {
        res.status(404).send();
    }
};
