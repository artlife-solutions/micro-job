//
// Shared microservices framework.
//

import { MicroService, IMicroServiceConfig, IMicroService, retry } from "@artlife/micro";

const inProduction = process.env.NODE_ENV === "production";

//
// Defines a job to be processed.
//
export interface IJob<PayloadT> {
    //
    // The ID of the job itself.
    //
    jobId: string;

    //
    // The job payload.
    //
    payload: PayloadT;
}


/**
 * Configures a microservice.
 */
export interface IMicroJobConfig extends IMicroServiceConfig {
    
}

/**
 * Defines a function to process a job.
 */
export type JobFn<PayloadT> = (service: IMicroJob, job: IJob<PayloadT>) => Promise<void>;

/**
 * Interface that represents a particular microservice instance.
 */
export interface IMicroJob extends IMicroService {

    /**
     * Register a function to process a named job.
     * This function will be called automatically when pending jobs are available in the job queue.
     * 
     * @param jobName The name of the job.
     */
    registerJob<PayloadT>(jobName: string, jobFn: JobFn<PayloadT>): void;

    /**
     * Submit a job to the job queue.
     * The job will be processed at some point in the future after waiting in the queue.
     * 
     * @param jobName The name of the job.
     */
    submitJobs<PayloadT>(jobName: string, jobs: PayloadT[]): Promise<void>;

}

const defaultConfig: IMicroJobConfig = {

};

//
// Class that represents a job-based microservice.
//
class MicroJob extends MicroService implements IMicroJob {

    //
    // Records when the microservice has started.
    //
    private started: boolean = false;

    //
    // TODO: Later allow different jobs to be registered.
    //
    private jobName?: string;
    private jobFn?: JobFn<any>;

    constructor(config?: IMicroJobConfig) {
        super(config);
    }

    /**
     * Register a function to process a named job.
     * This function will be called automatically when pending jobs are available in the job queue.
     * 
     * @param jobName The name of the job.
     */
    registerJob<PayloadT>(jobName: string, jobFn: JobFn<PayloadT>): void {
        if (this.jobName !== undefined) {
            throw new Error(`${jobName} has already been registsered. Currently only support registering a single job.`);
        }

        if (this.started) {
            throw new Error(`Please register jobs before calling the 'start' function.`);
        }

        this.jobName = jobName;
        this.jobFn = jobFn; 
    }

    /**
     * Submit a job to the job queue.
     * The job will be processed at some point in the future after waiting in the queue.
     * 
     * @param jobName The name of the job.
     */
    async submitJobs<PayloadT>(jobName: string, jobs: PayloadT[]): Promise<void> {
        if (jobs.length > 0) {
            await this.request.post("job-queue", "/submit-jobs", { 
                tag: jobName,
                jobs,
            });
        }
    }

    //
    // Start processing jobs.
    //
    private async processJobs(): Promise<void> {
        while (true) {
            console.log("Requesting next job.");
            const nextJob = await this.request.get("job-queue", `/pull-job?tag=${this.jobName}`);
            
            if (nextJob.ok) {
                console.log("Have a job to do.");
                
                const job: IJob<any> = nextJob.job;

                try {
                    await this.jobFn!(this, job);

                    //
                    // Let the job queue know that the job has completed.
                    //
                    await this.emit("job-completed", { jobId: job.jobId });
                }
                catch (err) {
                    console.error("Job failed, raising job-failed event.");
                    console.error(err && err.stack || err);
            
                    //
                    // Let the job queue know that the job has failed.
                    //
                    await this.emit("job-failed", { jobId: job.jobId, error: err.toString() });
                }
            }
            else {
                console.log("Sleeping.");
                console.log("Waiting for next job.");
                await this.waitForOneEvent("jobs-pending"); //todo: This message be specific for the job tag.
            }
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
export function micro(config?: IMicroJobConfig): IMicroJob {
    return new MicroJob(config);
}