const path = require("node:path");

require("dotenv").config({
    path: path.resolve(__dirname, "../../.env"),
});

const test = require("node:test");
const assert = require("node:assert/strict");
const mongoose = require("mongoose");

const Product = require("../../src/models/Product");
const Reservation = require("../../src/models/Reservation");
const OutboxEvent = require("../../src/models/OutboxEvent");

const {
    reserveInventory,
} = require("../../src/services/inventory.service");

const TEST_DB_NAME = "flashstock_test";

test(
    "creates one reservation for 20 simultaneous duplicate requests",
    async () => {
        assert.ok(
            process.env.MONGO_URI,
            "MONGO_URI must be configured in .env"
        );

        let connected = false;
        let productId = null;

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
                OutboxEvent.init(),
            ]);

            // Create a fresh product specifically for this test.
            const product = await Product.create({
                name: "Idempotency Race Test Product",
                sku: `TEST_IDEMPOTENCY_RACE_${Date.now()}`,
                price: 100,
                availableStock: 5,
                reservedStock: 0,
                soldStock: 0,
                isActive: true,
            });

            productId = product._id;

            // All 20 requests intentionally share the same key
            // and identical request data.
            const request = {
                productId: String(product._id),
                quantity: 1,
                idempotencyKey:
                    `test-idempotency-race-${Date.now()}`,
            };

            const results = await Promise.allSettled(
                Array.from(
                    { length: 20 },
                    () => reserveInventory(request)
                )
            );

            const failures = results.filter(
                (result) => result.status === "rejected"
            );

            assert.equal(
                failures.length,
                0,
                "All identical requests should succeed or replay"
            );

            const responses = results.map(
                (result) => result.value
            );

            const uniqueReservationIds = new Set(
                responses.map(
                    (result) => result.reservation.id
                )
            );

            assert.equal(
                uniqueReservationIds.size,
                1,
                "Every request should return the same reservation"
            );

            assert.equal(
                responses.filter(
                    (result) => result.replayed === false
                ).length,
                1,
                "Exactly one request should create the reservation"
            );

            assert.equal(
                responses.filter(
                    (result) => result.replayed === true
                ).length,
                19,
                "The other requests should replay the reservation"
            );

            // Verify the final persisted inventory.
            const updatedProduct = await Product.findById(
                product._id
            ).lean();

            assert.ok(updatedProduct);

            assert.equal(updatedProduct.availableStock, 4);
            assert.equal(updatedProduct.reservedStock, 1);
            assert.equal(updatedProduct.soldStock, 0);

            // Only this product's reservations count.
            const reservations = await Reservation.find({
                product: product._id,
            })
                .select("_id")
                .lean();

            assert.equal(reservations.length, 1);

            const reservationIds = reservations.map(
                (reservation) => reservation._id
            );

            // Verify the corresponding outbox event.
            const outboxEventCount =
                await OutboxEvent.countDocuments({
                    aggregateId: {
                        $in: reservationIds,
                    },
                });

            assert.equal(outboxEventCount, 1);

            console.log(
                "Concurrent idempotency test passed:",
                {
                    totalRequests: responses.length,
                    newReservations: 1,
                    replayedRequests: 19,
                    uniqueReservationIds:
                        uniqueReservationIds.size,
                    availableStock:
                        updatedProduct.availableStock,
                    reservedStock:
                        updatedProduct.reservedStock,
                    reservationCount:
                        reservations.length,
                    outboxEventCount,
                }
            );
        } finally {
            try {
                // Clean up only this test's records.
                if (
                    connected &&
                    mongoose.connection.readyState === 1 &&
                    mongoose.connection.db.databaseName ===
                        TEST_DB_NAME &&
                    productId
                ) {
                    const reservations =
                        await Reservation.find({
                            product: productId,
                        })
                            .select("_id")
                            .lean();

                    const reservationIds =
                        reservations.map(
                            (reservation) => reservation._id
                        );

                    if (reservationIds.length > 0) {
                        await OutboxEvent.deleteMany({
                            aggregateId: {
                                $in: reservationIds,
                            },
                        });
                    }

                    await Reservation.deleteMany({
                        product: productId,
                    });

                    await Product.deleteOne({
                        _id: productId,
                    });
                }
            } finally {
                if (mongoose.connection.readyState !== 0) {
                    await mongoose.disconnect();
                }
            }
        }
    }
);