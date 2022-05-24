import { v4 as uuidv4 } from 'uuid';
import { FlexTemplatesServiceClient } from '@google-cloud/dataflow';

const client = new FlexTemplatesServiceClient();

const location = 'us-central1';

export const launchJob = async (
    containerSpecGcsPath: string,
    parameters: Record<string, any>,
    jobName: string,
) => {
    const projectId = await client.getProjectId();

    return client
        .launchFlexTemplate({
            projectId,
            location,
            launchParameter: {
                jobName: `${jobName}-${uuidv4()}`,
                parameters,
                containerSpecGcsPath,
            },
        })
        .then(([job]) => <string>job.job?.id);
};
