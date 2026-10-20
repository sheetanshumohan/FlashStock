const {
    createOrderSchema,
    orderIdParamsSchema,
    idempotencyKeySchema,
} = require("../validators/order.validators");

const {
    createOrder: createOrderService,
    getOrderById,
} = require("../services/order.service");

const createOrder = async (req, res, next) => {
    // We do not permit the mock-payment system in production.
    if (
        process.env.NODE_ENV === "production" ||
        process.env.PAYMENT_MODE !== "mock"
    ) {
        return res.status(503).json({
            success: false,
            message:
                "Mock payments are available only in local development. Configure a real payment provider before production use.",
        });
    }

    const bodyResult = createOrderSchema.safeParse(req.body);

    const keyResult = idempotencyKeySchema.safeParse(
        req.get("Idempotency-Key")
    );

    if (!bodyResult.success || !keyResult.success) {
        return res.status(400).json({
            success: false,
            message: "Invalid order request",
            errors: [
                ...(bodyResult.success
                    ? []
                    : bodyResult.error.issues),
                ...(keyResult.success
                    ? []
                    : keyResult.error.issues),
            ].map((issue) => ({
                field: issue.path.join("."),
                message: issue.message,
            })),
        });
    }

    try {
        const result = await createOrderService({
            reservationId: bodyResult.data.reservationId,
            idempotencyKey: keyResult.data,
            testPaymentOutcome:
                bodyResult.data.testPaymentOutcome,
        });

        return res.status(result.replayed ? 200 : 202).json({
            success: true,
            message: result.replayed
                ? "Existing order returned"
                : "Order accepted for processing",
            data: result,
        });
    } catch (error) {
        return next(error);
    }
};

const getOrder = async (req, res, next) => {
    const paramsResult = orderIdParamsSchema.safeParse(
        req.params
    );

    if (!paramsResult.success) {
        return res.status(400).json({
            success: false,
            message: "Invalid order ID",
        });
    }

    try {
        const order = await getOrderById(
            paramsResult.data.orderId
        );

        return res.status(200).json({
            success: true,
            data: order,
        });
    } catch (error) {
        return next(error);
    }
};

module.exports = {
    createOrder,
    getOrder,
};