require("dotenv").config();
const connectDB = require("../config/db");
const mongoose = require("mongoose");
const { Worker } = require("bullmq");
const OutboxEvent = require("../models/OutboxEvent");
const { performance } = require("node:perf_hooks");
const {
    connectRedis,
    redisClient,
} = require("../config/redis");

const {
    getWorkerConnectionOptions,
} = require("../config/bullmq-connection");

const {
    releaseReservation,
} = require("../services/reservation.service");

const {
    reservationExpiryQueue,
} = require("../queues/reservation-expiry.queue");

const {
    recordOutboxQueueStatus,
} = require("../services/outbox-queue-status.service");

const updateOutboxProcessing = async (
    outboxEventId,
    fields,
    increment = {}
) => {
    // Allow older direct test jobs that have no Outbox event.
    if (!outboxEventId) {
        return;
    }

    if (!mongoose.isValidObjectId(outboxEventId)) {
        throw new Error(
            `Invalid Outbox event ID: ${outboxEventId}`
        );
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
const startWorker = async () => {
    await connectDB();
    await connectRedis();
    const worker = new Worker(
        "flashstock-reservation-expiry",


        async (job) => {
            if (job.name !== "expire-reservation") {
                throw new Error(
                    `Unsupported job type: ${job.name}`
                );
            }

            const {
                reservationId,
                outboxEventId,
            } = job.data;

            const startedAt = performance.now();

            console.log("[expiry] Starting job", {
                jobId: job.id,
                reservationId,
                outboxEventId,
            });

            try {
                // Record the start of downstream processing.
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

                // Perform the existing transactional inventory operation.
                const result = await releaseReservation(
                    reservationId
                );

                // Record successful business processing.
                await updateOutboxProcessing(
                    outboxEventId,
                    {
                        processingStatus: "succeeded",
                        processedAt: new Date(),
                        processingError: "",
                    }
                );

                console.log("[expiry] Job processed successfully", {
                    jobId: job.id,
                    reservationId,
                    outboxEventId,
                    status: result.reservation.status,
                    replayed: result.replayed,
                    durationMs: Math.round(performance.now() - startedAt),
                });

                return result;
            } catch (error) {
                const maxAttempts = Math.max(
                    1,
                    Number(job.opts.attempts ?? 1)
                );

                // attemptsMade counts failures before this invocation.
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
                        "[expiry] Could not update Outbox status:",
                        trackingError.message
                    );
                }

                console.error("[expiry] Processing failed", {
                    jobId: job.id,
                    reservationId,
                    outboxEventId,
                    attempt: currentAttempt,
                    maxAttempts,
                    processingStatus,
                    message: error.message,
                    durationMs: Math.round(performance.now() - startedAt),
                });

                // BullMQ needs a thrown Error to trigger retries.
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

    worker.on("completed", (job) => {
        void recordOutboxQueueStatus(job, "completed");

        console.log(`[worker] Completed job ${job.id}`);
    });

    worker.on("failed", (job, error) => {
        if (!job) {
            console.error(
                "[worker] Job failed without job details:",
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

        console.error(`[worker] Job ${job.id} failed`, {
            reservationId: job.data.reservationId,
            attemptsMade: job.attemptsMade,
            maxAttempts,
            queueStatus,
            reason: error.message,
        });
    });

    worker.on("stalled", (jobId) => {
        console.warn(
            `[worker] Job ${jobId} stalled and may be processed again`
        );

        void (async () => {
            try {
                const job = await reservationExpiryQueue.getJob(jobId);

                if (job) {
                    await recordOutboxQueueStatus(
                        job,
                        "stalled",
                        "BullMQ marked the job as stalled."
                    );
                }
            } catch (error) {
                console.error(
                    "[worker] Could not record stalled job status:",
                    error.message
                );
            }
        })();
    });
    let shuttingDown = false;

    const shutdown = async (signal) => {
        if (shuttingDown) return;

        shuttingDown = true;
        console.log(`[worker] Received ${signal}; shutting down`);

        try {
            await worker.close();
            await reservationExpiryQueue.close();

            if (redisClient.isOpen) {
                await redisClient.quit();
            }

            await mongoose.disconnect();

            process.exitCode = 0;
        } catch (error) {
            console.error(
                "[worker] Shutdown failed:",
                error.message
            );

            process.exitCode = 1;
        }
    };

    process.once("SIGINT", () => shutdown("SIGINT"));
    process.once("SIGTERM", () => shutdown("SIGTERM"));

    console.log("[worker] Reservation-expiry worker started");
};

startWorker().catch((error) => {
    console.error(
        "Worker startup failed:",
        error.message
    );

    process.exitCode = 1;
});
