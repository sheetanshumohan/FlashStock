const mongoose = require("mongoose");

const outboxEventSchema = new mongoose.Schema(
    {
        eventType: {
            type: String,
            enum: [
                "reservation.expiry.schedule",
                "order.process",
            ],
            required: true,
        },

        aggregateType: {
            type: String,
            enum: ["Reservation", "Order"],
            required: true,
        },

        aggregateId: {
            type: mongoose.Schema.Types.ObjectId,
            required: true,
        },

        // Unique business-event identifier.
        dedupeKey: {
            type: String,
            required: true,
            unique: true,
        },

        // Data required to publish the event.
        payload: {
            type: mongoose.Schema.Types.Mixed,
            required: true,
        },

        status: {
            type: String,
            enum: ["pending", "processing", "published"],
            default: "pending",
            required: true,
        },
        // Tracks business processing separately from queue publication.
        processingStatus: {
            type: String,
            enum: [
                "not_started",
                "processing",
                "retrying",
                "succeeded",
                "failed",
            ],
            default: "not_started",
            required: true,
        },

        // Number of times a downstream worker began processing the event.
        processingAttempts: {
            type: Number,
            default: 0,
            min: 0,
        },

        // When the downstream worker most recently started processing.
        processingStartedAt: {
            type: Date,
            default: null,
        },

        // When the business operation successfully completed.
        processedAt: {
            type: Date,
            default: null,
        },

        // Most recent downstream processing error.
        processingError: {
            type: String,
            default: "",
            maxlength: 1000,
        },
        // Tracks the BullMQ job lifecycle separately from
        // the business operation.
        queueStatus: {
            type: String,
            enum: [
                "not_enqueued",
                "queued",
                "active",
                "retrying",
                "completed",
                "failed",
                "stalled",
            ],
            default: "not_enqueued",
            required: true,
        },

        // BullMQ's attempt counters.
        queueAttemptsMade: {
            type: Number,
            default: 0,
            min: 0,
        },

        queueAttemptsAllowed: {
            type: Number,
            default: 0,
            min: 0,
        },

        // Queue lifecycle timestamps.
        queueStartedAt: {
            type: Date,
            default: null,
        },

        queueCompletedAt: {
            type: Date,
            default: null,
        },

        queueFailedAt: {
            type: Date,
            default: null,
        },

        // Most recent queue-level error.
        queueError: {
            type: String,
            default: "",
            maxlength: 1000,
        },
        attempts: {
            type: Number,
            default: 0,
            min: 0,
        },

        availableAt: {
            type: Date,
            default: Date.now,
            required: true,
        },

        lockedBy: {
            type: String,
            default: null,
        },

        lockedUntil: {
            type: Date,
            default: null,
        },

        publishedAt: {
            type: Date,
            default: null,
        },

        lastError: {
            type: String,
            default: "",
            maxlength: 1000,
        },
    },
    {
        timestamps: true,
    }
);

outboxEventSchema.index({
    status: 1,
    availableAt: 1,
    createdAt: 1,
});

outboxEventSchema.index({
    status: 1,
    lockedUntil: 1,
});

module.exports = mongoose.model(
    "OutboxEvent",
    outboxEventSchema
);