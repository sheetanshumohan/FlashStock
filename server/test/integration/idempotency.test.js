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

test("replays duplicate reservations without consuming stock twice", async () => {
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

        // Create a fresh product without deleting other test data.
        const product = await Product.create({
            name: "Idempotency Test Product",
            sku: `TEST_IDEMPOTENCY_${Date.now()}`,
            price: 100,
            availableStock: 5,
            reservedStock: 0,
            soldStock: 0,
            isActive: true,
        });

        productId = product._id;

        const request = {
            productId: String(product._id),
            quantity: 1,
            idempotencyKey:
                `test-idempotency-${Date.now()}`,
        };

        // First request creates the reservation.
        const first = await reserveInventory(request);

        assert.equal(first.replayed, false);

        // Second request returns the existing reservation.
        const second = await reserveInventory(request);

        assert.equal(second.replayed, true);
        assert.equal(
            second.reservation.id,
            first.reservation.id
        );

        // Reusing the key with different data must be rejected.
        await assert.rejects(
            () =>
                reserveInventory({
                    ...request,
                    quantity: 2,
                }),
            (error) => {
                assert.equal(error.statusCode, 409);

                assert.equal(
                    error.message,
                    "Idempotency-Key was already used with different reservation data"
                );

                return true;
            }
        );

        // Verify actual inventory.
        const updatedProduct = await Product.findById(
            product._id
        ).lean();

        assert.ok(updatedProduct);

        assert.equal(updatedProduct.availableStock, 4);
        assert.equal(updatedProduct.reservedStock, 1);
        assert.equal(updatedProduct.soldStock, 0);

        // Verify only this product's reservations.
        const reservations = await Reservation.find({
            product: product._id,
        })
            .select("_id")
            .lean();

        assert.equal(reservations.length, 1);

        const reservationIds = reservations.map(
            (reservation) => reservation._id
        );

        // Verify only the corresponding outbox events.
        const outboxEventCount =
            await OutboxEvent.countDocuments({
                aggregateId: { $in: reservationIds },
            });

        assert.equal(outboxEventCount, 1);

        console.log("Idempotency test passed:", {
            firstRequestReplayed: first.replayed,
            secondRequestReplayed: second.replayed,
            sameReservation:
                first.reservation.id === second.reservation.id,
            availableStock: updatedProduct.availableStock,
            reservedStock: updatedProduct.reservedStock,
            reservationCount: reservations.length,
            outboxEventCount,
        });
    } finally {
        try {
            // Remove only records owned by this test.
            if (
                connected &&
                mongoose.connection.readyState === 1 &&
                mongoose.connection.db.databaseName === TEST_DB_NAME &&
                productId
            ) {
                const reservations = await Reservation.find({
                    product: productId,
                })
                    .select("_id")
                    .lean();

                const reservationIds = reservations.map(
                    (reservation) => reservation._id
                );

                if (reservationIds.length > 0) {
                    await OutboxEvent.deleteMany({
                        aggregateId: { $in: reservationIds },
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
});