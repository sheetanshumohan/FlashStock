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
    "processes an order once when the same job is delivered twice",
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
                name: "Order Processing Test Product",
                sku: `TEST_ORDER_PROCESS_${Date.now()}`,
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
                    `test-order-reservation-${Date.now()}`,
            });

            reservationId = reservationResult.reservation.id;

            // Create the order using the real order service.
            const orderResult = await createOrder({
                reservationId,
                idempotencyKey:
                    `test-order-processing-${Date.now()}`,
                testPaymentOutcome: "success",
            });

            orderId = orderResult.order.id;

            assert.equal(orderResult.order.status, "payment_pending");

            // First delivery: process the order.
            const firstResult = await processOrder(orderId);

            assert.equal(firstResult.status, "confirmed");

            // Second delivery: simulate the same job being redelivered.
            const secondResult = await processOrder(orderId);

            assert.equal(secondResult.skipped, true);
            assert.equal(
                secondResult.reason,
                "order_already_final"
            );

            // Verify the persisted order.
            const savedOrder = await Order.findById(orderId).lean();

            assert.ok(savedOrder, "Order must still exist");

            assert.equal(savedOrder.status, "confirmed");
            assert.equal(savedOrder.paymentAttempts, 1);
            assert.ok(savedOrder.paidAt);

            // Verify the reservation was confirmed only once.
            const savedReservation =
                await Reservation.findById(reservationId).lean();

            assert.equal(savedReservation.status, "confirmed");

            // Verify the inventory was changed exactly once.
            const savedProduct =
                await Product.findById(productId).lean();

            assert.equal(savedProduct.availableStock, 4);
            assert.equal(savedProduct.reservedStock, 0);
            assert.equal(savedProduct.soldStock, 1);

            console.log("Order processing test passed:", {
                firstStatus: firstResult.status,
                secondDeliverySkipped: secondResult.skipped,
                finalOrderStatus: savedOrder.status,
                paymentAttempts: savedOrder.paymentAttempts,
                reservationStatus: savedReservation.status,
                availableStock: savedProduct.availableStock,
                reservedStock: savedProduct.reservedStock,
                soldStock: savedProduct.soldStock,
            });
        } finally {
            try {
                // Clean up only the records created by this test.
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