const mongoose = require("mongoose");

const orderSchema = new mongoose.Schema(
    {
        reservation: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "Reservation",
            required: true,
        },

        product: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "Product",
            required: true,
        },

        quantity: {
            type: Number,
            required: true,
            min: 1,
        },

        // Store money in paise, not floating-point rupees.
        totalAmountPaise: {
            type: Number,
            required: true,
            min: 0,
            validate: Number.isSafeInteger,
        },

        currency: {
            type: String,
            enum: ["INR"],
            default: "INR",
        },

        status: {
            type: String,
            enum: [
                "payment_pending",
                "processing",
                "confirmed",
                "payment_failed",
                "cancelled",
            ],
            default: "payment_pending",
            required: true,
        },

        idempotencyKey: {
            type: String,
            required: true,
            trim: true,
        },

        // Used only by the local mock payment implementation.
        testPaymentOutcome: {
            type: String,
            enum: ["success", "failure"],
            default: "success",
            select: false,
        },

        processingStartedAt: {
            type: Date,
            default: null,
        },

        paymentAttempts: {
            type: Number,
            default: 0,
            min: 0,
        },

        paidAt: {
            type: Date,
            default: null,
        },

        lastError: {
            type: String,
            default: "",
            maxlength: 500,
        },
    },
    {
        timestamps: true,
    }
);

// Only one order may be associated with a reservation.
orderSchema.index(
    { reservation: 1 },
    { unique: true }
);

// Retrying the same request must not create another order.
orderSchema.index(
    { idempotencyKey: 1 },
    { unique: true }
);

orderSchema.index({
    status: 1,
    createdAt: -1,
});

module.exports = mongoose.model("Order", orderSchema);