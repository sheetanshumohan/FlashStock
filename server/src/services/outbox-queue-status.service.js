const OutboxEvent = require("../models/OutboxEvent");

const recordOutboxQueueStatus = async (
    job,
    queueStatus,
    errorMessage = ""
) => {
    const outboxEventId = job?.data?.outboxEventId;

    // Support older or manually created jobs without an Outbox ID.
    if (!outboxEventId) {
        return;
    }

    const maxAttempts = job.opts?.attempts ?? 1;
    const now = new Date();

    const fields = {
        queueStatus,
        queueAttemptsMade: job.attemptsMade ?? 0,
        queueAttemptsAllowed: maxAttempts,
        queueError: String(errorMessage || "").slice(0, 1000),
    };

    if (queueStatus === "active") {
        fields.queueStartedAt = now;
        fields.queueCompletedAt = null;
        fields.queueFailedAt = null;
        fields.queueError = "";
    }

    if (queueStatus === "completed") {
        fields.queueCompletedAt = now;
        fields.queueFailedAt = null;
        fields.queueError = "";
    }

    if (queueStatus === "retrying" || queueStatus === "stalled") {
        fields.queueCompletedAt = null;
        fields.queueFailedAt = null;
    }

    if (queueStatus === "failed") {
        fields.queueFailedAt = now;
        fields.queueCompletedAt = null;
    }

    try {
        const filter = {
            _id: outboxEventId,
        };

        // A completed job must not regress to an earlier state.
        if (queueStatus !== "completed") {
            filter.queueStatus = {
                $ne: "completed",
            };
        }

        const result = await OutboxEvent.updateOne(
            filter,
            { $set: fields }
        );

        if (result.matchedCount !== 1) {
            console.warn(
                `[outbox-status] Event ${outboxEventId} not found`
            );
        }
    } catch (error) {
        // Status-tracking errors must not interrupt the worker.
        console.error(
            `[outbox-status] Could not update event ${outboxEventId}:`,
            error.message
        );
    }
};

module.exports = {
    recordOutboxQueueStatus,
};