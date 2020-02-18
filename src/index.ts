//
// Shared microservices framework.
//

import { MicroService, IMicroServiceConfig, IMicroService, retry } from "@artlife/micro";

const inProduction = process.env.NODE_ENV === "production";

//
// Arguments to the register-jobs REST API.
//
export interface IRegisterAssetJobsArgs {
    //
    // Jobs to be registered.
    //
    jobs: IJobDetails<IJob>[];
}

//
// Defines a job that can be requested and processed.
//
export interface IJob {
    //
    // The ID of the job itself.
    //
    jobId: string;
}

//
// Defines an asset-based job that can be requested and processed.
//
export interface IAssetJob extends IJob {

    //
    // The ID of the user who contributed the asset.
    //
    userId: string;

    //
    // The ID of the asset to be classified.
    //
    assetId: string;

    //
    // The ID of the account that owns the asset.
    //
    accountId: string;

    //
    // The mimetype of the asset.
    //
    mimeType: string;

    //
    // The encoding of the asset.
    //
    encoding: string;
}

//
// Arguments to the job-complete message.
//
export interface IJobCompletedArgs {

    //
    // The ID of the job that was successfully completed.
    //
    jobId: string;
}

//
// Arguments to the job-failed message.
//
export interface IJobFailedArgs {

    //
    // The ID of the job that failed.
    //
    jobId: string;

    //
    // The error that caused the job failure.
    //
    error: any;
}

//
// Result returned by the request-job REST API.
//
export interface IRequestJobResult {
    //
    // Set to true if a job is avaialble for processing.
    //
    ok: boolean;

    //
    // The next job in the queue, if ok is set to true.
    //
    job?: IJob;
}

/**
 * Configures a microservice.
 */
export interface IMicroJobConfig extends IMicroServiceConfig {
    
}

/**
 * Defines a function to process a job.
 */
export type JobFn<JobT> = (service: IMicroJob, job: JobT) => Promise<void>;

/**
 * Interface that represents a particular microservice instance.
 */
export interface IMicroJob extends IMicroService {

    /**
     * Register a function to process a job.
     * 
     * This function will be called automatically when pending jobs are available in the job queue.
     * 
     * @param jobName The name of the job.
     */
    registerJob<JobT>(jobDetails: IJobDetails<JobT>): Promise<void>;

}

//
// Define the job.
//
export interface IJobDetails<JobT> {
    //
    // Name of the job.
    //
    jobName: string;

    //
    // Name of the service that handles the job.
    //
    serviceName?: string;

    //
    // The function that is executed to process the job.
    //
    jobFn: JobFn<JobT>;
}

//
// Defines an asset-based job.
//
export interface IAssetJobDetails extends IJobDetails<IAssetJob> {
    //
    // The mimetype of the assets that this job processes.
    //
    mimeType: string; //todo: is this needed?
}

//
// Class that represents a job-based microservice.
//
class MicroJob extends MicroService implements IMicroJob {

    //
    // Records when the microservice has started.
    //
    private started: boolean = false;

    //
    // Registered jobs.
    //
    private jobList: IJobDetails<IJob>[] = [];

    constructor(config: IMicroJobConfig) {
        super(config);
    }

    /**
     * Register a function to process a named job.
     * This function will be called automatically when pending jobs are available in the job queue.
     * 
     * @param jobName The name of the job.
     */
    async registerJob<JobT>(jobDetails: IJobDetails<JobT>): Promise<void> {
        if (this.started) {
            throw new Error(`Please register jobs before calling the 'start' function.`);
        }

        jobDetails = Object.assign({}, jobDetails);
        jobDetails.serviceName = this.getServiceName();

        const registerJobsArgs: IRegisterAssetJobsArgs = {
            jobs: [ jobDetails as any as IJobDetails<IJob> ],
        }

        await retry(() => this.request.post("job-queue", "/register-jobs", registerJobsArgs), 10, 1000);
    }

    //
    // Start processing jobs.
    //
    private async processJobs(): Promise<void> {
        console.log(`Commencing job processing loop with ${this.jobList.length} registered jobs.`);

        if (this.jobList.length === 0) {
            throw new Error("No jobs registered with micro-job!");
        }

        while (true) {
            for (const jobDetails of this.jobList) {
                console.log("Requesting next job.");
                const route = `/request-job?job=${jobDetails.jobName}&service=${this.getServiceName()}&id=${this.getInstanceId()}`;
                const response = await retry(() => this.request.get("job-queue", route), 10, 1000);
                const requestJobResult: IRequestJobResult = response.data;                
                if (requestJobResult.ok) {
                    console.log("Have a job to do.");
                    
                    const job = requestJobResult.job!;

                    try {
                        await jobDetails.jobFn(this, job);

                        //
                        // Let the job queue know that the job has completed.
                        //
                        const jobCompletedArgs: IJobCompletedArgs = { jobId: job.jobId! };
                        await this.emit("job-completed", jobCompletedArgs);
                    }
                    catch (err) {
                        console.error("Job failed, raising job-failed event.");
                        console.error(err && err.stack || err);
                
                        //
                        // Let the job queue know that the job has failed.
                        //
                        const jobFailedArgs: IJobFailedArgs = { jobId: job.jobId!, error: err.toString() };
                        await this.emit("job-failed", jobFailedArgs);
                    }
                }
            }

            console.log("Waiting for next job.");
            await this.waitForOneEvent("jobs-pending");
        }
    }
    

    /**
     * Starts the microservice.
     * It starts listening for incoming HTTP requests and events.
     */
    async start(): Promise<void> {
        await super.start();

        console.log("Starting job processing.");

        //TODO: Should have a retry forever function. Need to report an error if can't connect after 5 minutes, also want to exponentially back off.
        retry(() => this.processJobs(), 10000, 1000 * 60)
            .catch(err => {
                console.error("Failed to start job processing.");
                console.error(err && err.stack || err);
            });

        this.started = true;
    }
}

/**
 * Instantiates a jobs-based microservice.
 * 
 * @param [config] Optional configuration for the microservice.
 */
export function micro(config: IMicroJobConfig): IMicroJob {
    return new MicroJob(config);
}