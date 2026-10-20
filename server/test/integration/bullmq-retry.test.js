const path = require("node:path");

require("dotenv").config({
    path: path.resolve(__dirname, "../../.env"),
});

const test = require("node:test");
const assert = require("node:assert/strict");
const { Queue, Worker } = require("bullmq");

const {
    getQueueConnectionOptions,
    getWorkerConnectionOptions,
} = require("../../src/config/bullmq-connection");

test(
    "BullMQ retries failed jobs and completes them after recovery",
    async () => {
        const queueName =
            `flashstock-test-retry-${process.pid}-${Date.now()}`;

        const jobId =
            `retry-test-${process.pid}-${Date.now()}`;

        let queue;
        let worker;
        let executions = 0;
        let completedHandler;
        let failedHandler;
        let timeoutId;

        try {
            queue = new Queue(queueName, {
                connection: getQueueConnectionOptions(),
            });

            worker = new Worker(
                queueName,
                async (job) => {
                    executions += 1;

                    console.log("Test worker execution:", {
                        jobId: job.id,
                        execution: executions,
                    });

                    // Deliberately fail twice to test retries.
                    if (executions < 3) {
                        throw new Error(
                            `Intentional test failure ${executions}`
                        );
                    }

                    return {
                        success: true,
                        executions,
                    };
                },
                {
                    connection: getWorkerConnectionOptions(),
                    concurrency: 1,
                }
            );

            await Promise.all([
                queue.waitUntilReady(),
                worker.waitUntilReady(),
            ]);

            // Listen before adding the job so we don't miss completion.
            const completionPromise = new Promise(
                (resolve, reject) => {
                    const cleanup = () => {
                        clearTimeout(timeoutId);

                        worker.off(
                            "completed",
                            completedHandler
                        );

                        worker.off(
                            "failed",
                            failedHandler
                        );
                    };

                    timeoutId = setTimeout(() => {
                        cleanup();

                        reject(
                            new Error(
                                "Timed out waiting for BullMQ retries"
                            )
                        );
                    }, 20000);

                    timeoutId.unref?.();

                    completedHandler = (job, result) => {
                        if (job.id !== jobId) return;

                        cleanup();
                        resolve(result);
                    };

                    failedHandler = (job, error) => {
                        if (job?.id !== jobId) return;

                        const maxAttempts = Number(
                            job.opts.attempts || 1
                        );

                        if (job.attemptsMade >= maxAttempts) {
                            cleanup();
                            reject(error);
                        }
                    };

                    worker.on(
                        "completed",
                        completedHandler
                    );

                    worker.on("failed", failedHandler);
                }
            );

            await queue.add(
                "retry-test",
                { purpose: "verify-retry" },
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

            const result = await completionPromise;

            assert.equal(result.success, true);
            assert.equal(result.executions, 3);
            assert.equal(executions, 3);

            const savedJob = await queue.getJob(jobId);

            assert.ok(savedJob, "Completed test job should exist");

            const finalState = await savedJob.getState();

            assert.equal(finalState, "completed");

            console.log("BullMQ retry test passed:", {
                processorExecutions: executions,
                finalJobState: finalState,
                result,
            });
        } finally {
            if (timeoutId) {
                clearTimeout(timeoutId);
            }

            if (worker) {
                if (completedHandler) {
                    worker.off("completed", completedHandler);
                }

                if (failedHandler) {
                    worker.off("failed", failedHandler);
                }

                await worker.close();
            }

            if (queue) {
                try {
                    // The queue name is unique to this test.
                    await queue.obliterate({ force: true });
                } finally {
                    await queue.close();
                }
            }
        }
    }
);