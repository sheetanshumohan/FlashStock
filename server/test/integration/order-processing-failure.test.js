const path = require("node:path");

require("dotenv").config({
    path: path.resolve(__dirname, "../../.env"),
});

const test = require("node:test");
const assert = require("node:assert/strict");
const mongoose = require("mongoose");

const Product = require("../../src/models/Product");
const Reservation = require("../../src/models/Reservation");
const Order = require("../../src/models/Order");
const OutboxEvent = require("../../src/models/OutboxEvent");

const {
    reserveInventory,
} = require("../../src/services/inventory.service");

const {
    createOrder,
} = require("../../src/services/order.service");

const {
    processOrder,
} = require("../../src/services/order-processing.service");

const TEST_DB_NAME = "flashstock_test";

test(
    "releases reserved inventory exactly once when payment fails",
    async () => {
        assert.ok(
            process.env.MONGO_URI,
            "MONGO_URI must be configured in .env"
        );

        let connected = false;
        let productId = null;
        let reservationId = null;
        let orderId = null;

        try {
            await mongoose.connect(process.env.MONGO_URI, {
                dbName: TEST_DB_NAME,
                serverSelectionTimeoutMS: 10000,
            });

            connected = true;

            assert.equal(
                mongoose.connection.db.databaseName,
                TEST_DB_NAME,
                "Refusing to test against a non-test database"
            );

            await Promise.all([
                Product.init(),
                Reservation.init(),
                Order.init(),
                OutboxEvent.init(),
            ]);

            // Create a product with five available units.
            const product = await Product.create({
                name: "Payment Failure Test Product",
                sku: `TEST_PAYMENT_FAILURE_${Date.now()}`,
                price: 100,
                availableStock: 5,
                reservedStock: 0,
                soldStock: 0,
                isActive: true,
            });

            productId = product._id;

            // Reserve one unit.
            const reservationResult = await reserveInventory({
                productId: String(product._id),
                quantity: 1,
                idempotencyKey:
                    `test-failure-reservation-${Date.now()}`,
            });

            reservationId = reservationResult.reservation.id;

            // Create an order with the deterministic mock failure.
            const orderResult = await createOrder({
                reservationId,
                idempotencyKey:
                    `test-failure-order-${Date.now()}`,
                testPaymentOutcome: "failure",
            });

            orderId = orderResult.order.id;

            assert.equal(
                orderResult.order.status,
                "payment_pending"
            );

            // First execution should release the reservation.
            const firstResult = await processOrder(orderId);

            assert.equal(firstResult.status, "payment_failed");

            // Repeated delivery must not release inventory again.
            const secondResult = await processOrder(orderId);

            assert.equal(secondResult.skipped, true);
            assert.equal(
                secondResult.reason,
                "order_already_final"
            );

            const savedOrder = await Order.findById(orderId)
                .select("+testPaymentOutcome")
                .lean();

            assert.ok(savedOrder);
            assert.equal(savedOrder.status, "payment_failed");
            assert.equal(savedOrder.paymentAttempts, 1);
            assert.equal(
                savedOrder.lastError,
                "Simulated payment failure"
            );
            assert.equal(savedOrder.paidAt, null);

            const savedReservation =
                await Reservation.findById(reservationId).lean();

            assert.ok(savedReservation);
            assert.equal(savedReservation.status, "released");

            const savedProduct =
                await Product.findById(productId).lean();

            assert.ok(savedProduct);
            assert.equal(savedProduct.availableStock, 5);
            assert.equal(savedProduct.reservedStock, 0);
            assert.equal(savedProduct.soldStock, 0);

            console.log("Payment failure test passed:", {
                firstStatus: firstResult.status,
                secondDeliverySkipped: secondResult.skipped,
                paymentAttempts: savedOrder.paymentAttempts,
                reservationStatus: savedReservation.status,
                availableStock: savedProduct.availableStock,
                reservedStock: savedProduct.reservedStock,
                soldStock: savedProduct.soldStock,
            });
        } finally {
            try {
                // Clean up only records created by this test.
                if (
                    connected &&
                    mongoose.connection.readyState === 1 &&
                    mongoose.connection.db.databaseName ===
                        TEST_DB_NAME
                ) {
                    const aggregateIds = [
                        reservationId,
                        orderId,
                    ]
                        .filter(Boolean)
                        .map(
                            (id) => new mongoose.Types.ObjectId(id)
                        );

                    if (aggregateIds.length > 0) {
                        await OutboxEvent.deleteMany({
                            aggregateId: {
                                $in: aggregateIds,
                            },
                        });
                    }

                    if (orderId) {
                        await Order.deleteOne({
                            _id: orderId,
                        });
                    }

                    if (reservationId) {
                        await Reservation.deleteOne({
                            _id: reservationId,
                        });
                    }

                    if (productId) {
                        await Product.deleteOne({
                            _id: productId,
                        });
                    }
                }
            } finally {
                if (mongoose.connection.readyState !== 0) {
                    await mongoose.disconnect();
                }
            }
        }
    }
);