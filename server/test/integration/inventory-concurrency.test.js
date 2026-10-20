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

test("prevents overselling under 20 concurrent reservations", async (t) => {
    assert.ok(
        process.env.MONGO_URI,
        "MONGO_URI must be configured in the local .env file"
    );

    // Explicitly select the dedicated test database.
    await mongoose.connect(process.env.MONGO_URI, {
        dbName: TEST_DB_NAME,
        serverSelectionTimeoutMS: 10000,
    });

    try {
        assert.equal(
            mongoose.connection.db.databaseName,
            TEST_DB_NAME,
            "Refusing to test against a non-test database"
        );

        // Ensure the indexes exist before testing concurrency.
        await Promise.all([
            Product.init(),
            Reservation.init(),
            OutboxEvent.init(),
        ]);

        // Clean only these collections in flashstock_test.
        await Promise.all([
            Product.deleteMany({}),
            Reservation.deleteMany({}),
            OutboxEvent.deleteMany({}),
        ]);

        const product = await Product.create({
            name: "Concurrency Test Product",
            sku: `TEST_CONCURRENCY_${Date.now()}`,
            price: 100,
            availableStock: 5,
            reservedStock: 0,
            soldStock: 0,
            isActive: true,
        });

        const runId = Date.now().toString();

        const results = await Promise.all(
            Array.from({ length: 20 }, (_, index) =>
                reserveInventory({
                    productId: String(product._id),
                    quantity: 1,
                    idempotencyKey:
                        `test-concurrency-${runId}-${index}`,
                }).then(
                    (value) => ({ success: true, value }),
                    (error) => ({ success: false, error })
                )
            )
        );

        const successful = results.filter(
            (result) => result.success
        );

        const rejected = results.filter(
            (result) => !result.success
        );

        // Exactly five reservations should succeed.
        assert.equal(successful.length, 5);

        // The other fifteen must be rejected for insufficient stock.
        assert.equal(rejected.length, 15);

        assert.ok(
            rejected.every(
                ({ error }) =>
                    error.statusCode === 409 &&
                    error.message === "Insufficient available stock"
            ),
            "Every rejected request should report insufficient stock"
        );

        // Verify persisted inventory, not just returned responses.
        const updatedProduct = await Product.findById(
            product._id
        ).lean();

        assert.equal(updatedProduct.availableStock, 0);
        assert.equal(updatedProduct.reservedStock, 5);
        assert.equal(updatedProduct.soldStock, 0);

        assert.equal(
            await Reservation.countDocuments({
                product: product._id,
            }),
            5
        );

        assert.equal(
            await OutboxEvent.countDocuments({}),
            5
        );

        console.log("Inventory concurrency test passed:", {
            successfulReservations: successful.length,
            insufficientStockResponses: rejected.length,
            availableStock: updatedProduct.availableStock,
            reservedStock: updatedProduct.reservedStock,
            soldStock: updatedProduct.soldStock,
        });
    } finally {
        // Never clear data unless we're connected to the test database.
        if (
            mongoose.connection.readyState === 1 &&
            mongoose.connection.db.databaseName === TEST_DB_NAME
        ) {
            await Promise.all([
                Product.deleteMany({}),
                Reservation.deleteMany({}),
                OutboxEvent.deleteMany({}),
            ]);
        }

        if (mongoose.connection.readyState !== 0) {
            await mongoose.disconnect();
        }
    }
});