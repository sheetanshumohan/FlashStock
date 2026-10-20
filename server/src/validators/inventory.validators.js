const { z } = require("zod");

const createReservationSchema = z.object({
    quantity: z
        .number()
        .int("Quantity must be a whole number")
        .positive("Quantity must be greater than zero")
        .max(1_000_000),
}).strict();

const reservationParamsSchema = z.object({
    productId: z
        .string()
        .regex(/^[0-9a-fA-F]{24}$/, "Invalid product ID"),
});
const idempotencyKeySchema = z
    .string()
    .trim()
    .min(8, "Idempotency-Key must be at least 8 characters")
    .max(128, "Idempotency-Key cannot exceed 128 characters")
    .regex(
        /^[A-Za-z0-9._:-]+$/,
        "Invalid Idempotency-Key format"
    );
module.exports = {
    createReservationSchema,
    reservationParamsSchema,
    idempotencyKeySchema
};