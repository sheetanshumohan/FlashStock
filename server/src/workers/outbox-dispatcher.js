require("dotenv").config();

const crypto = require("node:crypto");
const mongoose = require("mongoose");

const connectDB = require("../config/db");

const OutboxEvent = require("../models/OutboxEvent");

const {
    reservationExpiryQueue,
    scheduleReservationExpiry,
} = require("../queues/reservation-expiry.queue");

const {
    orderProcessingQueue,
    enqueueOrderProcessing,
} = require("../queues/order-processing.queue");

const workerId = `${process.pid}-${crypto.randomUUID()}`;

const LOCK_DURATION_MS = 60_000;
const POLL_INTERVAL_MS = 500;

let stopping = false;

const sleep = (ms) =>
    new Promise((resolve) => setTimeout(resolve, ms));

const markOutboxQueued = async (event, job) => {
    try {
        await OutboxEvent.updateOne(
            {
                _id: event._id,
                queueStatus: "not_enqueued",
            },
            {
                $set: {
                    queueStatus: "queued",
                    queueAttemptsMade: job.attemptsMade ?? 0,
                    queueAttemptsAllowed: job.opts?.attempts ?? 1,
                    queueError: "",
                },
            }
        );
    } catch (error) {
        // Queue-status tracking must not interrupt publication.
        console.error(
            `[outbox] Could not record queued status for ${event._id}:`,
            error.message
        );
    }
};
const claimNextEvent = async () => {
    const now = new Date();

    return OutboxEvent.findOneAndUpdate(
        {
            $or: [
                {
                    status: "pending",
                    availableAt: { $lte: now },
                },
                {
                    status: "processing",
                    lockedUntil: { $lte: now },
                },
            ],
        },
        {
            $set: {
                status: "processing",
                lockedBy: workerId,
                lockedUntil: new Date(
                    now.getTime() + LOCK_DURATION_MS
                ),
            },
            $inc: {
                attempts: 1,
            },
        },
        {
            sort: {
                availableAt: 1,
                createdAt: 1,
            },
            returnDocument: "after",
        }
    ).lean();
};

const publishEvent = async (event) => {
    try {
        switch (event.eventType) {
            case "reservation.expiry.schedule":{
                const expiresAt = new Date(event.payload.expiresAt);

                const delayMs = Math.max(
                    0,
                    expiresAt.getTime() - Date.now()
                );

                console.log("[outbox] Scheduling reservation expiry", {
                    eventId: String(event._id),
                    reservationId: event.payload.reservationId,
                    expiresAt: expiresAt.toISOString(),
                    delayMs,
                });

                const job = await scheduleReservationExpiry({
                    reservationId: event.payload.reservationId,
                    expiresAt,
                    outboxEventId: String(event._id),
                });
                await markOutboxQueued(event, job);
                console.log("[outbox] Reservation expiry job queued", {
                    jobId: job.id,
                    state: await job.getState(),
                    delay: job.opts.delay,
                });

                break;
            }
            case "order.process":
            const job = await enqueueOrderProcessing(
                event.payload.orderId,
                String(event._id)
            );

            await markOutboxQueued(event, job);
            break;

            default:
                throw new Error(
                    `Unsupported outbox event: ${event.eventType}`
                );
        }

        const updateResult = await OutboxEvent.updateOne(
            {
                _id: event._id,
                status: "processing",
                lockedBy: workerId,
            },
            {
                $set: {
                    status: "published",
                    publishedAt: new Date(),
                    lastError: "",
                },
                $unset: {
                    lockedBy: 1,
                    lockedUntil: 1,
                },
            }
        );

        if (updateResult.modifiedCount === 0) {
            console.warn(
                `[outbox] Event ${event._id} was published, ` +
                "but this dispatcher no longer owns its lease"
            );
            return;
        }

        console.log(
            `[outbox] Published ${event.eventType}: ${event._id}`
        );
    } catch (error) {
        const retryDelayMs = Math.min(
            1000 * 2 ** Math.min(event.attempts - 1, 6),
            60_000
        );

        try {
            await OutboxEvent.updateOne(
                {
                    _id: event._id,
                    status: "processing",
                    lockedBy: workerId,
                },
                {
                    $set: {
                        status: "pending",
                        availableAt: new Date(
                            Date.now() + retryDelayMs
                        ),
                        lastError: String(
                            error.message || error
                        ).slice(0, 1000),
                    },
                    $unset: {
                        lockedBy: 1,
                        lockedUntil: 1,
                    },
                }
            );
        } catch (updateError) {
            console.error(
                "[outbox] Could not release event lease:",
                updateError.message
            );
        }

        console.error(
            `[outbox] Publishing ${event._id} failed:`,
            error.message
        );
    }
};

const startDispatcher = async () => {
    await connectDB();

    console.log(
        `[outbox] Dispatcher ${workerId} started`
    );

    while (!stopping) {
        try {
            const event = await claimNextEvent();

            if (event) {
                await publishEvent(event);
            } else {
                await sleep(POLL_INTERVAL_MS);
            }
        } catch (error) {
            console.error(
                "[outbox] Dispatcher loop error:",
                error.message
            );

            await sleep(1000);
        }
    }

    await Promise.allSettled([
        reservationExpiryQueue.close(),
        orderProcessingQueue.close(),
    ]);

    await mongoose.disconnect();

    console.log("[outbox] Dispatcher stopped");
};

const requestShutdown = (signal) => {
    console.log(`[outbox] Received ${signal}`);
    stopping = true;
};

process.once("SIGINT", () => requestShutdown("SIGINT"));
process.once("SIGTERM", () => requestShutdown("SIGTERM"));

startDispatcher().catch((error) => {
    console.error(
        "[outbox] Startup failed:",
        error.message
    );

    process.exitCode = 1;
});