const path = require("node:path");

require("dotenv").config({
    path: path.resolve(__dirname, "../../.env"),
});

const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const { setTimeout: sleep } = require("node:timers/promises");
const { Queue, Worker } = require("bullmq");

const {
    getQueueConnectionOptions,
    getWorkerConnectionOptions,
} = require("../../src/config/bullmq-connection");

test(
    "recovers an active job after its worker process is killed",
    async () => {
        const queueName =
            `flashstock-crash-test-${process.pid}-${Date.now()}`;

        const jobId =
            `crash-recovery-${process.pid}-${Date.now()}`;

        let queue;
        let child;
        let recoveryWorker;

        let childOutput = "";
        let childErrors = "";
        let recoveryExecutions = 0;

        const childWorkerCode = `
            const { Worker } = require("bullmq");

            const {
                getWorkerConnectionOptions
            } = require("./src/config/bullmq-connection");

            const worker = new Worker(
                process.env.FLASHSTOCK_TEST_QUEUE,
                async (job) => {
                    // The parent kills this process while this
                    // job is active and holding its BullMQ lock.
                    console.log("JOB_STARTED:" + job.id);

                    await new Promise(() => {});
                },
                {
                    connection: getWorkerConnectionOptions(),
                    concurrency: 1,
                    lockDuration: 3000,
                    stalledInterval: 1000,
                    maxStalledCount: 2
                }
            );

            worker.on("error", (error) => {
                console.error("CHILD_WORKER_ERROR:", error.message);
            });

            worker.waitUntilReady()
                .then(() => console.log("WORKER_READY"))
                .catch((error) => {
                    console.error("CHILD_START_ERROR:", error.message);
                    process.exitCode = 1;
                });
        `;

        const waitForChildMarker = async (
            marker,
            timeoutMs = 10000
        ) => {
            const deadline = Date.now() + timeoutMs;

            while (Date.now() < deadline) {
                if (childOutput.includes(marker)) {
                    return;
                }

                if (
                    child.exitCode !== null ||
                    child.signalCode !== null
                ) {
                    throw new Error(
                        `Worker exited before "${marker}". ` +
                        `stdout: ${childOutput}; ` +
                        `stderr: ${childErrors}`
                    );
                }

                await sleep(50);
            }

            throw new Error(
                `Timed out waiting for "${marker}". ` +
                `stdout: ${childOutput}; ` +
                `stderr: ${childErrors}`
            );
        };

        try {
            queue = new Queue(queueName, {
                connection: getQueueConnectionOptions(),
            });

            await queue.waitUntilReady();

            // Start the first worker in a separate Node.js process.
            child = spawn(
                process.execPath,
                ["-e", childWorkerCode],
                {
                    cwd: process.cwd(),
                    env: {
                        ...process.env,
                        FLASHSTOCK_TEST_QUEUE: queueName,
                    },
                    stdio: ["ignore", "pipe", "pipe"],
                }
            );

            child.stdout.on("data", (chunk) => {
                childOutput += chunk.toString();
            });

            child.stderr.on("data", (chunk) => {
                childErrors += chunk.toString();
            });

            // Don't enqueue until the child worker is ready.
            await waitForChildMarker("WORKER_READY");

            await queue.add(
                "crash-recovery-test",
                { purpose: "simulate-worker-crash" },
                {
                    jobId,
                    attempts: 3,
                    backoff: {
                        type: "exponential",
                        delay: 100,
                    },
                    removeOnComplete: false,
                    removeOnFail: false,
                }
            );

            // This marker proves the job entered the processor.
            await waitForChildMarker(`JOB_STARTED:${jobId}`);

            console.log(
                "First worker acquired the job; terminating it."
            );

            // Kill the actual child process.
            const childExit = new Promise((resolve) => {
                if (
                    child.exitCode !== null ||
                    child.signalCode !== null
                ) {
                    resolve();
                } else {
                    child.once("exit", resolve);
                }
            });

            child.kill();
            await childExit;

            console.log(
                "First worker stopped. Starting replacement worker."
            );

            // This replacement worker processes the stalled job.
            recoveryWorker = new Worker(
                queueName,
                async (job) => {
                    recoveryExecutions += 1;

                    console.log(
                        "Replacement worker processed:",
                        job.id
                    );

                    return {
                        recovered: true,
                        jobId: job.id,
                    };
                },
                {
                    connection: getWorkerConnectionOptions(),
                    concurrency: 1,
                    lockDuration: 3000,
                    stalledInterval: 1000,
                    maxStalledCount: 2,
                }
            );

            recoveryWorker.on("error", (error) => {
                console.error(
                    "[crash-recovery-test] Worker error:",
                    error.message
                );
            });

            await recoveryWorker.waitUntilReady();

            // Wait until BullMQ detects the expired lock and
            // the replacement worker completes the job.
            const deadline = Date.now() + 20000;
            let finalState = "unknown";
            let completedJob = null;

            while (Date.now() < deadline) {
                completedJob = await queue.getJob(jobId);

                if (!completedJob) {
                    throw new Error(
                        "Test job unexpectedly disappeared from the queue"
                    );
                }

                finalState = await completedJob.getState();

                if (finalState === "completed") {
                    break;
                }

                if (finalState === "failed") {
                    throw new Error(
                        "Job entered the failed state before recovery completed"
                    );
                }

                await sleep(200);
            }

            assert.equal(
                finalState,
                "completed",
                "Replacement worker should complete the recovered job"
            );

            assert.equal(
                recoveryExecutions,
                1,
                "Replacement worker should process the recovered job once"
            );

            const refreshedJob = await queue.getJob(jobId);

            assert.ok(
                refreshedJob,
                "Completed recovery job should still exist"
            );

            assert.equal(
                await refreshedJob.getState(),
                "completed"
            );

            assert.deepEqual(
                refreshedJob.returnvalue,
                {
                    recovered: true,
                    jobId,
                }
            );

            console.log("BullMQ crash recovery test passed:", {
                jobId,
                originalWorkerKilled: true,
                replacementExecutions: recoveryExecutions,
                finalJobState: finalState,
            });
        } finally {
            // Clean up only this test's child process, worker and queue.
            if (
                child &&
                child.exitCode === null &&
                child.signalCode === null
            ) {
                const childExit = new Promise((resolve) => {
                    child.once("exit", resolve);
                });

                child.kill();
                await childExit;
            }

            if (recoveryWorker) {
                await recoveryWorker.close();
            }

            if (queue) {
                try {
                    await queue.obliterate({ force: true });
                } finally {
                    await queue.close();
                }
            }
        }
    }
);