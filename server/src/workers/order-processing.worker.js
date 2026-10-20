require("dotenv").config();

const { Worker } = require("bullmq");

const connectDB = require("../config/db");
const mongoose = require("mongoose");
const OutboxEvent = require("../models/OutboxEvent");
const {
    getWorkerConnectionOptions,
} = require("../config/bullmq-connection");

const {
    orderProcessingQueue,
} = require("../queues/order-processing.queue");

const {
    recordOutboxQueueStatus,
} = require("../services/outbox-queue-status.service");

const {
    connectRedis,
    redisClient,
} = require("../config/redis");

const updateOutboxProcessing = async (
    outboxEventId,
    fields,
    increment = {}
) => {
    if (!outboxEventId) {
        return;
    }

    const update = {
        $set: fields,
    };

    if (Object.keys(increment).length > 0) {
        update.$inc = increment;
    }

    const result = await OutboxEvent.updateOne(
        { _id: outboxEventId },
        update
    );

    if (result.matchedCount !== 1) {
        throw new Error(
            `Outbox event not found: ${outboxEventId}`
        );
    }
};
const {
    processOrder,
} = require("../services/order-processing.service");

const startWorker = async () => {
    await connectDB();
    await connectRedis();

    const worker = new Worker(
        process.env.ORDER_PROCESSING_QUEUE_NAME ||
        "flashstock-order-processing",

    async (job) => {
            if (job.name !== "process-order") {
                throw new Error(
                    `Unsupported job type: ${job.name}`
                );
            }

            const {
                orderId,
                outboxEventId,
            } = job.data;

            const startedAt = Date.now();

            console.log("[order-worker] Processing order", {
                jobId: job.id,
                orderId,
                outboxEventId,
            });

            try {
                await updateOutboxProcessing(
                    outboxEventId,
                    {
                        processingStatus: "processing",
                        processingStartedAt: new Date(),
                        processingError: "",
                    },
                    {
                        processingAttempts: 1,
                    }
                );

                // Run your existing order-processing service.
                const result = await processOrder(orderId);

                await updateOutboxProcessing(
                    outboxEventId,
                    {
                        processingStatus: "succeeded",
                        processedAt: new Date(),
                        processingError: "",
                    }
                );

                console.log("[order-worker] Processing succeeded", {
                    jobId: job.id,
                    orderId,
                    outboxEventId,
                    durationMs: Date.now() - startedAt,
                });

                return result;
            } catch (error) {
                const maxAttempts = Math.max(
                    1,
                    Number(job.opts.attempts ?? 1)
                );

                const currentAttempt =
                    Number(job.attemptsMade ?? 0) + 1;

                const processingStatus =
                    currentAttempt >= maxAttempts
                        ? "failed"
                        : "retrying";

                try {
                    await updateOutboxProcessing(
                        outboxEventId,
                        {
                            processingStatus,
                            processingError: String(
                                error.message || error
                            ).slice(0, 1000),
                        }
                    );
                } catch (trackingError) {
                    console.error(
                        "[order-worker] Could not update Outbox status:",
                        trackingError.message
                    );
                }

                console.error("[order-worker] Processing failed", {
                    jobId: job.id,
                    orderId,
                    outboxEventId,
                    attempt: currentAttempt,
                    maxAttempts,
                    processingStatus,
                    message: error.message,
                });

                // Preserve BullMQ retries by rethrowing the error.
                throw error;
            }
        },
        {
            connection: getWorkerConnectionOptions(),
            concurrency: 5,
        }
    );

    worker.on("active", (job) => {
        void recordOutboxQueueStatus(job, "active");
    });

    worker.on("completed", (job, result) => {
        void recordOutboxQueueStatus(job, "completed");

        console.log(
            `[order-worker] Job ${job.id} completed`,
            result
        );
    });

    worker.on("failed", (job, error) => {
        if (!job) {
            console.error(
                "[order-worker] Job failed without job details:",
                error.message
            );
            return;
        }

        const maxAttempts = job.opts.attempts ?? 1;

        const finalFailure =
            job.attemptsMade >= maxAttempts;

        const queueStatus = finalFailure
            ? "failed"
            : "retrying";

        void recordOutboxQueueStatus(
            job,
            queueStatus,
            error.message
        );

        console.error(
            `[order-worker] Job ${job.id} failed`,
            {
                orderId: job.data.orderId,
                attemptsMade: job.attemptsMade,
                maxAttempts,
                queueStatus,
                reason: error.message,
            }
        );
    });

    worker.on("stalled", (jobId) => {
        console.warn(
            `[order-worker] Job ${jobId} stalled and may be processed again`
        );

        void (async () => {
            try {
                const job = await orderProcessingQueue.getJob(jobId);

                if (job) {
                    await recordOutboxQueueStatus(
                        job,
                        "stalled",
                        "BullMQ marked the job as stalled."
                    );
                }
            } catch (error) {
                console.error(
                    "[order-worker] Could not record stalled job status:",
                    error.message
                );
            }
        })();
    });

    let shuttingDown = false;

    const shutdown = async (signal) => {
        if (shuttingDown) return;

        shuttingDown = true;
        console.log(`[order-worker] Received ${signal}`);

        try {
            await worker.close();

            if (redisClient.isOpen) {
                await redisClient.quit();
            }

            await mongoose.disconnect();

            process.exitCode = 0;
        } catch (error) {
            console.error(
                "[order-worker] Shutdown failed:",
                error.message
            );

            process.exitCode = 1;
        }
    };

    process.once("SIGINT", () => shutdown("SIGINT"));
    process.once("SIGTERM", () => shutdown("SIGTERM"));

    console.log("[order-worker] Worker started");
};

startWorker().catch((error) => {
    console.error(
        "[order-worker] Startup failed:",
        error.message
    );

    process.exitCode = 1;
});