const { z } = require("zod");

const createProductSchema = z.object({
    name: z.string().trim().min(2).max(120),

    sku: z
        .string()
        .trim()
        .min(3)
        .max(40)
        .regex(/^[A-Za-z0-9_-]+$/, "Invalid SKU format"),

    description: z.string().trim().max(1000).optional().default(""),

    price: z.number().finite().nonnegative(),

    initialStock: z
        .number()
        .int()
        .nonnegative()
        .max(1_000_000),
}).strict();

module.exports = {
    createProductSchema,
};