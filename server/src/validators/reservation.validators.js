const { z } = require("zod");

const reservationIdParamsSchema = z.object({
    reservationId: z
        .string()
        .regex(/^[0-9a-fA-F]{24}$/, "Invalid reservation ID"),
}).strict();

module.exports = {
    reservationIdParamsSchema,
};