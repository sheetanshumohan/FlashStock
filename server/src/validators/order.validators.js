const { z } = require("zod");

const createOrderSchema = z.object({
    reservationId: z
        .string()
        .regex(/^[a-fA-F0-9]{24}$/, "Invalid reservation ID"),

    testPaymentOutcome: z
        .enum(["success", "failure"])
        .optional()
        .default("success"),
}).strict();

const orderIdParamsSchema = z.object({
    orderId: z
        .string()
        .regex(/^[a-fA-F0-9]{24}$/, "Invalid order ID"),
}).strict();

const idempotencyKeySchema = z
    .string()
    .min(8)
    .max(128)
    .regex(/^[A-Za-z0-9._:-]+$/);

module.exports = {
    createOrderSchema,
    orderIdParamsSchema,
    idempotencyKeySchema,
};