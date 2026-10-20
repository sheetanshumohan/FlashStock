const Order = require("../models/Order");
const Reservation = require("../models/Reservation");

const {
    confirmReservation,
    releaseReservation,
} = require("./reservation.service");

const FINAL_ORDER_STATUSES = new Set([
    "confirmed",
    "payment_failed",
    "cancelled",
]);

// A stale processing claim may be recovered after this lease expires.
// Keep this below the worker's expected stalled-job recovery interval.
const PROCESSING_LEASE_MS = Math.max(
    1000,
    Number(process.env.ORDER_PROCESSING_LEASE_MS) || 20000
);

const createHttpError = (statusCode, message) => {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
};

const markOrder = async (
    orderId,
    processingStartedAt,
    updates
) => {
    // Only the execution holding this exact claim may finalize it.
    const updated = await Order.findOneAndUpdate(
        {
            _id: orderId,
            status: "processing",
            processingStartedAt,
        },
        {
            $set: updates,
        },
        {
            returnDocument: "after",
        }
    ).lean();

    if (updated) {
        return updated;
    }

    const current = await Order.findById(orderId).lean();

    if (!current) {
        throw new Error(`Order not found: ${orderId}`);
    }

    // Another execution may already have completed the order.
    if (FINAL_ORDER_STATUSES.has(current.status)) {
        return current;
    }

    throw new Error(
        `Lost processing claim before finalizing order ${orderId}`
    );
};

const buildSkippedResult = (orderId, reason) => ({
    skipped: true,
    reason,
    orderId: String(orderId),
});

const processOrder = async (orderId) => {
    const order = await Order.findById(orderId)
        .select("+testPaymentOutcome")
        .lean();

    if (!order) {
        throw new Error(`Order not found: ${orderId}`);
    }

    if (FINAL_ORDER_STATUSES.has(order.status)) {
        console.log(
            `[order-worker] Skipping already-final order ${order._id}`
        );

        return buildSkippedResult(
            order._id,
            "order_already_final"
        );
    }

    const now = new Date();

    const staleBefore = new Date(
        now.getTime() - PROCESSING_LEASE_MS
    );

    // A fresh claim belongs to another execution.
    if (
        order.status === "processing" &&
        order.processingStartedAt &&
        order.processingStartedAt > staleBefore
    ) {
        return buildSkippedResult(
            order._id,
            "order_already_processing"
        );
    }

    let claimFilter;

    if (order.status === "payment_pending") {
        // Normal claim: only one execution can change this state.
        claimFilter = {
            _id: order._id,
            status: "payment_pending",
        };
    } else if (order.status === "processing") {
        // Recover an abandoned claim after its lease has expired.
        claimFilter = {
            _id: order._id,
            status: "processing",
            $or: [
                {
                    processingStartedAt: {
                        $lte: staleBefore,
                    },
                },
                {
                    processingStartedAt: null,
                },
            ],
        };
    } else {
        throw new Error(
            `Unexpected order status: ${order.status}`
        );
    }

    // Claim and increment the attempt counter atomically.
    const claimedOrder = await Order.findOneAndUpdate(
        claimFilter,
        {
            $set: {
                status: "processing",
                processingStartedAt: new Date(),
            },
            $inc: {
                paymentAttempts: 1,
            },
        },
        {
            returnDocument: "after",
        }
    )
        .select("+testPaymentOutcome")
        .lean();

    if (!claimedOrder) {
        // Another execution may have claimed or completed the order
        // after our initial read.
        const latestOrder = await Order.findById(orderId)
            .select("+testPaymentOutcome")
            .lean();

        if (!latestOrder) {
            throw new Error(`Order not found: ${orderId}`);
        }

        if (FINAL_ORDER_STATUSES.has(latestOrder.status)) {
            return buildSkippedResult(
                latestOrder._id,
                "order_already_final"
            );
        }

        if (latestOrder.status === "processing") {
            return buildSkippedResult(
                latestOrder._id,
                "order_already_processing"
            );
        }

        // Let the queue retry unexpected claim races.
        throw new Error(
            `Could not claim order ${orderId}; current status is ${latestOrder.status}`
        );
    }

    const claimTimestamp = claimedOrder.processingStartedAt;

    try {
        // Deterministic mock payment failure.
        if (claimedOrder.testPaymentOutcome === "failure") {
            await releaseReservation(claimedOrder.reservation);

            const updated = await markOrder(
                claimedOrder._id,
                claimTimestamp,
                {
                    status: "payment_failed",
                    processingStartedAt: null,
                    lastError: "Simulated payment failure",
                }
            );

            return {
                orderId: String(claimedOrder._id),
                status: updated.status,
            };
        }

        try {
            await confirmReservation(claimedOrder.reservation);
        } catch (error) {
            if (error.statusCode === 409) {
                const reservation = await Reservation.findById(
                    claimedOrder.reservation
                ).lean();

                if (
                    reservation &&
                    reservation.status === "pending" &&
                    reservation.expiresAt <= new Date()
                ) {
                    // The expiration worker may not have run yet.
                    await releaseReservation(reservation._id);
                }

                const latestReservation =
                    await Reservation.findById(
                        claimedOrder.reservation
                    ).lean();

                if (
                    latestReservation &&
                    ["expired", "released"].includes(
                        latestReservation.status
                    )
                ) {
                    const updated = await markOrder(
                        claimedOrder._id,
                        claimTimestamp,
                        {
                            status: "cancelled",
                            processingStartedAt: null,
                            lastError:
                                "Reservation expired or was released before confirmation",
                        }
                    );

                    return {
                        orderId: String(claimedOrder._id),
                        status: updated.status,
                    };
                }
            }

            throw error;
        }

        const updated = await markOrder(
            claimedOrder._id,
            claimTimestamp,
            {
                status: "confirmed",
                processingStartedAt: null,
                paidAt: new Date(),
                lastError: "",
            }
        );

        return {
            orderId: String(claimedOrder._id),
            status: updated.status,
        };
    } catch (error) {
        // For a recoverable error, return the order to pending so a
        // queue retry can claim it again. The timestamp condition
        // prevents resetting a claim now owned by another execution.
        try {
            await Order.updateOne(
                {
                    _id: claimedOrder._id,
                    status: "processing",
                    processingStartedAt: claimTimestamp,
                },
                {
                    $set: {
                        status: "payment_pending",
                        processingStartedAt: null,
                    },
                }
            );
        } catch (resetError) {
            console.error(
                "[order-worker] Could not reset processing claim:",
                resetError.message
            );
        }

        throw error;
    }
};

module.exports = {
    processOrder,
};