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
    "processes concurrent duplicate order jobs without double-counting payment attempts",
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

            const product = await Product.create({
                name: "Concurrent Order Test Product",
                sku: `TEST_ORDER_CONCURRENT_${Date.now()}`,
                price: 100,
                availableStock: 5,
                reservedStock: 0,
                soldStock: 0,
                isActive: true,
            });

            productId = product._id;

            const reservationResult = await reserveInventory({
                productId: String(product._id),
                quantity: 1,
                idempotencyKey:
                    `test-order-concurrent-res-${Date.now()}`,
            });

            reservationId = reservationResult.reservation.id;

            const orderResult = await createOrder({
                reservationId,
                idempotencyKey:
                    `test-order-concurrent-${Date.now()}`,
                testPaymentOutcome: "success",
            });

            orderId = orderResult.order.id;

            // Start both executions without waiting for either one.
            const results = await Promise.allSettled([
                processOrder(orderId),
                processOrder(orderId),
            ]);

            const failures = results.filter(
                (result) => result.status === "rejected"
            );

            // Both executions should finish without an error.
            assert.equal(
                failures.length,
                0,
                `Concurrent processing errors: ${failures
                    .map((result) => result.reason?.message)
                    .join("; ")}`
            );

            const savedOrder = await Order.findById(orderId).lean();

            assert.ok(savedOrder);
            assert.equal(savedOrder.status, "confirmed");

            // Both executions must not count as separate payments.
            assert.equal(savedOrder.paymentAttempts, 1);

            const savedReservation =
                await Reservation.findById(reservationId).lean();

            assert.ok(savedReservation);
            assert.equal(savedReservation.status, "confirmed");

            const savedProduct =
                await Product.findById(productId).lean();

            assert.ok(savedProduct);
            assert.equal(savedProduct.availableStock, 4);
            assert.equal(savedProduct.reservedStock, 0);
            assert.equal(savedProduct.soldStock, 1);

            const orderOutboxCount =
                await OutboxEvent.countDocuments({
                    aggregateId: new mongoose.Types.ObjectId(orderId),
                    eventType: "order.process",
                });

            assert.equal(orderOutboxCount, 1);

            console.log("Concurrent order-processing test passed:", {
                executions: results.length,
                fulfilledExecutions:
                    results.filter(
                        (result) => result.status === "fulfilled"
                    ).length,
                orderStatus: savedOrder.status,
                paymentAttempts: savedOrder.paymentAttempts,
                reservationStatus: savedReservation.status,
                availableStock: savedProduct.availableStock,
                reservedStock: savedProduct.reservedStock,
                soldStock: savedProduct.soldStock,
                orderOutboxEvents: orderOutboxCount,
            });
        } finally {
            try {
                // Remove only records created by this test.
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