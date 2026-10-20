const mongoose = require("mongoose");

const reservationSchema = new mongoose.Schema(
    {
        product: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "Product",
            required: true,
        },

        quantity: {
            type: Number,
            required: true,
            min: 1,
            validate: Number.isSafeInteger,
        },

        status: {
            type: String,
            enum: ["pending", "confirmed", "released", "expired"],
            default: "pending",
            required: true,
        },

        idempotencyKey: {
            type: String,
            required: true,
            trim: true,
            minlength: 8,
            maxlength: 128,
        },

        inventorySnapshot: {
            availableStock: {
                type: Number,
                required: true,
                min: 0,
            },
            reservedStock: {
                type: Number,
                required: true,
                min: 0,
            },
            soldStock: {
                type: Number,
                required: true,
                min: 0,
            },
        },

        expiresAt: {
            type: Date,
            required: true,
        },
    },
    {
        timestamps: true,
    }
);

reservationSchema.index({
    status: 1,
    expiresAt: 1,
});
reservationSchema.index(
    { idempotencyKey: 1 },
    {
        unique: true,
        partialFilterExpression: {
            idempotencyKey: { $type: "string" },
        },
    }
);

module.exports = mongoose.model("Reservation", reservationSchema);