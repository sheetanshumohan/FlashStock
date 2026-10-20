const path = require("node:path");

require("dotenv").config({
    path: path.resolve(__dirname, "../../.env"),
});

// Configure the short lease before importing the service.
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
    confirmReservation,
} = require("../../src/services/reservation.service");

const {
    processOrder,
} = require("../../src/services/order-processing.service");

const TEST_DB_NAME = "flashstock_test";

test(
    "recovers an order when inventory confirmation succeeded before a worker crash",
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
                name: "Post-Effect Recovery Test Product",
                sku: `TEST_POST_EFFECT_${Date.now()}`,
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
                    `test-post-effect-reservation-${Date.now()}`,
            });

            reservationId = reservationResult.reservation.id;

            const orderResult = await createOrder({
                reservationId,
                idempotencyKey:
                    `test-post-effect-order-${Date.now()}`,
                testPaymentOutcome: "success",
            });

            orderId = orderResult.order.id;

            // Simulate the order worker claiming the order.
            const staleTimestamp = new Date(Date.now() - 5000);

            const claimResult = await Order.updateOne(
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

            assert.equal(claimResult.modifiedCount, 1);

            // Simulate the inventory side effect committing just
            // before the worker crashes, leaving the order unfinished.
            const confirmation = await confirmReservation(
                reservationId
            );

            assert.equal(
                confirmation.reservation.status,
                "confirmed"
            );

            const productAfterConfirmation =
                await Product.findById(productId).lean();

            assert.equal(
                productAfterConfirmation.availableStock,
                4
            );

            assert.equal(
                productAfterConfirmation.reservedStock,
                0
            );

            assert.equal(
                productAfterConfirmation.soldStock,
                1
            );

            // The order is still processing. The next worker should
            // recover the stale claim and finish the order.
            const recoveryResult = await processOrder(orderId);

            assert.equal(
                recoveryResult.status,
                "confirmed"
            );

            const savedOrder = await Order.findById(
                orderId
            ).lean();

            assert.equal(savedOrder.status, "confirmed");
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

            // Inventory must not be changed by confirmation twice.
            const finalProduct = await Product.findById(
                productId
            ).lean();

            assert.equal(finalProduct.availableStock, 4);
            assert.equal(finalProduct.reservedStock, 0);
            assert.equal(finalProduct.soldStock, 1);

            // Delivering the completed order again must be harmless.
            const duplicateResult = await processOrder(orderId);

            assert.equal(duplicateResult.skipped, true);

            const finalOrder = await Order.findById(
                orderId
            ).lean();

            assert.equal(finalOrder.paymentAttempts, 2);

            console.log("Post-effect recovery test passed:", {
                orderStatus: finalOrder.status,
                paymentAttempts: finalOrder.paymentAttempts,
                reservationStatus: savedReservation.status,
                availableStock: finalProduct.availableStock,
                reservedStock: finalProduct.reservedStock,
                soldStock: finalProduct.soldStock,
                duplicateDeliverySkipped: duplicateResult.skipped,
            });
        } finally {
            try {
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