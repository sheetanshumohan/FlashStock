const test = require("node:test");
const assert = require("node:assert/strict");

const {
    createProductSchema,
} = require("../../src/validators/product.validators");

const {
    createReservationSchema,
    reservationParamsSchema,
    idempotencyKeySchema,
} = require("../../src/validators/inventory.validators");

const {
    createOrderSchema,
    orderIdParamsSchema,
} = require("../../src/validators/order.validators");

test("accepts a valid product and defaults its description", () => {
    const result = createProductSchema.safeParse({
        name: "Test Product",
        sku: "TEST_001",
        price: 100,
        initialStock: 10,
    });

    assert.equal(result.success, true);

    if (result.success) {
        assert.equal(result.data.description, "");
    }
});

test("rejects invalid product data", () => {
    const result = createProductSchema.safeParse({
        name: "Test Product",
        sku: "INVALID/SKU",
        price: -100,
        initialStock: 10,
    });

    assert.equal(result.success, false);
});

test("accepts positive integer reservation quantities", () => {
    assert.equal(
        createReservationSchema.safeParse({ quantity: 2 }).success,
        true
    );

    assert.equal(
        createReservationSchema.safeParse({ quantity: 0 }).success,
        false
    );

    assert.equal(
        createReservationSchema.safeParse({ quantity: 1.5 }).success,
        false
    );
});

test("validates product IDs used for reservations", () => {
    assert.equal(
        reservationParamsSchema.safeParse({
            productId: "507f1f77bcf86cd799439011",
        }).success,
        true
    );

    assert.equal(
        reservationParamsSchema.safeParse({
            productId: "invalid-id",
        }).success,
        false
    );
});

test("validates order payloads and payment outcomes", () => {
    const reservationId = "507f1f77bcf86cd799439011";

    const result = createOrderSchema.safeParse({
        reservationId,
    });

    assert.equal(result.success, true);

    if (result.success) {
        assert.equal(result.data.testPaymentOutcome, "success");
    }

    assert.equal(
        createOrderSchema.safeParse({
            reservationId,
            testPaymentOutcome: "pending",
        }).success,
        false
    );
});

test("validates order IDs and idempotency keys", () => {
    assert.equal(
        orderIdParamsSchema.safeParse({
            orderId: "507f1f77bcf86cd799439011",
        }).success,
        true
    );

    assert.equal(
        idempotencyKeySchema.safeParse(
            "reservation-test-001"
        ).success,
        true
    );

    assert.equal(
        idempotencyKeySchema.safeParse("short").success,
        false
    );

    assert.equal(
        idempotencyKeySchema.safeParse(
            "reservation key with spaces"
        ).success,
        false
    );
});