const path = require("node:path");

require("dotenv").config({
    path: path.resolve(__dirname, "../../.env"),
});

// Use a short lease only inside this test process.
// This does not change your actual .env file.
process.env.ORDER_PROCESSING_LEASE_MS = "1000";

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
    "recovers a stale processing order after a simulated worker interruption",
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

            // Create a dedicated test product.
            const product = await Product.create({
                name: "Order Recovery Test Product",
                sku: `TEST_ORDER_RECOVERY_${Date.now()}`,
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
                    `test-recovery-reservation-${Date.now()}`,
            });

            reservationId = reservationResult.reservation.id;

            // Create a pending order.
            const orderResult = await createOrder({
                reservationId,
                idempotencyKey:
                    `test-recovery-order-${Date.now()}`,
                testPaymentOutcome: "success",
            });

            orderId = orderResult.order.id;

            // Simulate a worker that claimed the order and then stopped.
            const staleTimestamp = new Date(
                Date.now() - 5000
            );

            await Order.updateOne(
                {
                    _id: orderId,
                    status: "payment_pending",
                },
                {
                    $set: {
                        status: "processing",
                        processingStartedAt: staleTimestamp,
                    },
                    $inc: {
                        paymentAttempts: 1,
                    },
                }
            );

            const abandonedOrder = await Order.findById(
                orderId
            ).lean();

            assert.equal(abandonedOrder.status, "processing");
            assert.equal(abandonedOrder.paymentAttempts, 1);

            // A new execution should reclaim the expired processing lease.
            const result = await processOrder(orderId);

            assert.equal(result.status, "confirmed");

            const savedOrder = await Order.findById(
                orderId
            ).lean();

            assert.equal(savedOrder.status, "confirmed");

            // The recovery was a second processing attempt.
            assert.equal(savedOrder.paymentAttempts, 2);
            assert.equal(savedOrder.processingStartedAt, null);

            const savedReservation =
                await Reservation.findById(
                    reservationId
                ).lean();

            assert.equal(
                savedReservation.status,
                "confirmed"
            );

            const savedProduct = await Product.findById(
                productId
            ).lean();

            assert.equal(savedProduct.availableStock, 4);
            assert.equal(savedProduct.reservedStock, 0);
            assert.equal(savedProduct.soldStock, 1);

            // Delivering again must not sell another unit.
            const duplicateResult = await processOrder(orderId);

            assert.equal(duplicateResult.skipped, true);

            const finalProduct = await Product.findById(
                productId
            ).lean();

            assert.equal(finalProduct.availableStock, 4);
            assert.equal(finalProduct.reservedStock, 0);
            assert.equal(finalProduct.soldStock, 1);

            console.log("Order recovery test passed:", {
                recoveredOrderStatus: savedOrder.status,
                paymentAttempts: savedOrder.paymentAttempts,
                reservationStatus: savedReservation.status,
                availableStock: finalProduct.availableStock,
                reservedStock: finalProduct.reservedStock,
                soldStock: finalProduct.soldStock,
                duplicateDeliverySkipped: duplicateResult.skipped,
            });
        } finally {
            try {
                // Delete only this test's records.
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