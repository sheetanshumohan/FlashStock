const mongoose = require("mongoose");

const Order = require("../models/Order");
const Product = require("../models/Product");
const Reservation = require("../models/Reservation");
const OutboxEvent = require("../models/OutboxEvent");


const createHttpError = (statusCode, message) => {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
};

const serializeOrder = (order) => ({
    id: String(order._id),
    reservation: String(order.reservation),
    product: String(order.product),
    quantity: order.quantity,
    totalAmountPaise: order.totalAmountPaise,
    currency: order.currency,
    status: order.status,
    paymentAttempts: order.paymentAttempts,
    paidAt: order.paidAt,
    lastError: order.lastError,
    createdAt: order.createdAt,
    updatedAt: order.updatedAt,
});


const validateReplay = (
    order,
    reservationId,
    testPaymentOutcome
) => {
    const sameReservation =
        String(order.reservation) === String(reservationId);

    const sameOutcome =
        (order.testPaymentOutcome || "success") ===
        testPaymentOutcome;

    if (!sameReservation || !sameOutcome) {
        throw createHttpError(
            409,
            "Idempotency-Key was already used for different order data"
        );
    }

    return {
        order: serializeOrder(order),
        replayed: true,
    };
};

const createOrder = async ({
    reservationId,
    idempotencyKey,
    testPaymentOutcome = "success",
}) => {
    // Fast path for request retries.
    const existing = await Order.findOne({
        idempotencyKey,
    })
        .select("+testPaymentOutcome")
        .lean();

    if (existing) {
        const replay = validateReplay(
            existing,
            reservationId,
            testPaymentOutcome
        );

        return replay;
    }

    const session = await mongoose.startSession();

    let createdOrder;

    try {
        await session.withTransaction(async () => {
            const reservation = await Reservation.findById(
                reservationId
            ).session(session);

            if (!reservation) {
                throw createHttpError(
                    404,
                    "Reservation not found"
                );
            }

            if (reservation.status !== "pending") {
                throw createHttpError(
                    409,
                    "Only pending reservations can be ordered"
                );
            }

            if (reservation.expiresAt <= new Date()) {
                throw createHttpError(
                    409,
                    "Reservation has expired"
                );
            }

            const product = await Product.findById(
                reservation.product
            ).session(session);

            if (!product) {
                throw createHttpError(
                    409,
                    "Reservation product no longer exists"
                );
            }

            const unitPricePaise = Math.round(
                product.price * 100
            );

            const totalAmountPaise =
                unitPricePaise * reservation.quantity;

            if (!Number.isSafeInteger(totalAmountPaise)) {
                throw createHttpError(
                    400,
                    "Order amount exceeds the supported range"
                );
            }

            const [order] = await Order.create(
                [
                    {
                        reservation: reservation._id,
                        product: product._id,
                        quantity: reservation.quantity,
                        totalAmountPaise,
                        currency: "INR",
                        status: "payment_pending",
                        idempotencyKey,
                        testPaymentOutcome,
                    },
                ],
                { session }
            );
            await OutboxEvent.create(
            [
                {
                    eventType: "order.process",
                    aggregateType: "Order",
                    aggregateId: order._id,

                    dedupeKey: `order-process-${order._id}`,

                    payload: {
                        orderId: String(order._id),
                    },

                    status: "pending",
                },
            ],
            { session }
        );

            createdOrder = {
                _id: order._id,
                reservation: order.reservation,
                product: order.product,
                quantity: order.quantity,
                totalAmountPaise: order.totalAmountPaise,
                currency: order.currency,
                status: order.status,
                idempotencyKey: order.idempotencyKey,
                testPaymentOutcome: order.testPaymentOutcome,
                paymentAttempts: order.paymentAttempts,
                paidAt: order.paidAt,
                lastError: order.lastError,
                createdAt: order.createdAt,
                updatedAt: order.updatedAt,
            };
        });
    } catch (error) {
        if (error.code === 11000) {
            const existingByKey = await Order.findOne({
                idempotencyKey,
            })
                .select("+testPaymentOutcome")
                .lean();

            if (existingByKey) {
                const replay = validateReplay(
                    existingByKey,
                    reservationId,
                    testPaymentOutcome
                );

                return replay;
            }

            const existingByReservation = await Order.findOne({
                reservation: reservationId,
            }).lean();

            if (existingByReservation) {
                throw createHttpError(
                    409,
                    "An order already exists for this reservation"
                );
            }
        }

        throw error;
    } finally {
        await session.endSession();
    }


    return {
        order: serializeOrder(createdOrder),
        replayed: false,
    };
};

const getOrderById = async (orderId) => {
    const order = await Order.findById(orderId).lean();

    if (!order) {
        throw createHttpError(404, "Order not found");
    }

    return serializeOrder(order);
};

module.exports = {
    createOrder,
    getOrderById,
};