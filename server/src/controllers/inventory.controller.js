const {
    createReservationSchema,
    reservationParamsSchema,
    idempotencyKeySchema
} = require("../validators/inventory.validators");

const {
    reserveInventory,
} = require("../services/inventory.service");

const createReservation = async (req, res, next) => {
    const paramsResult = reservationParamsSchema.safeParse(req.params);
    const bodyResult = createReservationSchema.safeParse(req.body);
    const keyResult = idempotencyKeySchema.safeParse(req.get("Idempotency-Key"));

    if (!paramsResult.success || !bodyResult.success) {
        return res.status(400).json({
            success: false,
            message: "Invalid reservation request",
            errors: [
                ...(paramsResult.success
                    ? []
                    : paramsResult.error.issues),
                ...(bodyResult.success
                    ? []
                    : bodyResult.error.issues),
            ].map((issue) => ({
                field: issue.path.join("."),
                message: issue.message,
            })),
        });
    }
    if (!keyResult.success) {
        return res.status(400).json({
            success: false,
            message: "A valid Idempotency-Key header is required",
            errors: keyResult.error.issues.map((issue) => ({
                field: "Idempotency-Key",
                message: issue.message,
            })),
        });
    }
    try {
        const result = await reserveInventory({
            productId: paramsResult.data.productId,
            quantity: bodyResult.data.quantity,
            idempotencyKey:keyResult.data
        });

        return res.status(result.replayed ? 200 : 201).json({
            success: true,
            message: result.replayed
                ? "Existing reservation returned"
                : "Inventory reserved successfully",
            result,
        });
    } catch (error) {
        next(error);
    }
};

module.exports = {
    createReservation,
};