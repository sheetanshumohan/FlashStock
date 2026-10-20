const mongoose = require("mongoose");

const Product = require("../models/Product");
const Reservation = require("../models/Reservation");
const OutboxEvent = require("../models/OutboxEvent");
const { performance } = require("node:perf_hooks");
const logReservationPerformance =
    process.env.RESERVATION_PERFORMANCE_LOGS === "true";
const {
    invalidateProductListCache,
} = require("./product-cache.service");

const createHttpError = (statusCode, message) => {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
};

const inFlightReservations = new Map();

const serializeReservation = (reservation) => ({
    id: String(reservation._id),
    product: String(reservation.product),
    quantity: reservation.quantity,
    status: reservation.status,
    expiresAt: reservation.expiresAt,
});

const buildReplayResult = (existing, productId, quantity) => {
    const sameProduct =
        String(existing.product) === String(productId);

    const sameQuantity = existing.quantity === quantity;

    if (!sameProduct || !sameQuantity) {
        throw createHttpError(
            409,
            "Idempotency-Key was already used with different reservation data"
        );
    }

    return {
        reservation: serializeReservation(existing),
        inventory: existing.inventorySnapshot,
        replayed: true,
    };
};

const reserveInventoryInternal = async ({
    productId,
    quantity,
    idempotencyKey,
}) => {
    const requestStartedAt = performance.now();

    const readyStateBeforeSession =
    mongoose.connection.readyState;

    const sessionStartedAt = performance.now();

    const session = await mongoose.startSession();

    const sessionStartMs =
        performance.now() - sessionStartedAt;

    const readyStateAfterSession =
        mongoose.connection.readyState;

    let productUpdateMs = 0;
    let reservationOutboxPrepareMs = 0;
    let reservationOutboxBulkWriteMs = 0;
    let transactionMs = 0;
    let cacheInvalidationMs = 0;
    let transactionCallbackMs = 0;

    try {

        let result;
        const transactionStartedAt = performance.now();

        await session.withTransaction(async () => {
            // Atomically check and move available stock to reserved stock.
            const productUpdateStartedAt = performance.now();
            const transactionCallbackStartedAt = performance.now();

            const updatedProduct = await Product.findOneAndUpdate(
                {
                    _id: productId,
                    isActive: true,
                    availableStock: { $gte: quantity },
                },
                {
                    $inc: {
                        availableStock: -quantity,
                        reservedStock: quantity,
                    },
                },
                {
                    returnDocument: "after",
                    session,
                }
            ) .select("_id availableStock reservedStock soldStock")
                .lean();

            productUpdateMs =
                performance.now() - productUpdateStartedAt;

                if (!updatedProduct) {
                    const existingReservation = await Reservation.findOne({
                    idempotencyKey,
                })
                    .session(session)
                    .lean();

                if (existingReservation) {
                    result = buildReplayResult(
                        existingReservation,
                        productId,
                        quantity
                    );

                    return;
                }
                const product = await Product.findById(productId)
                    .select("isActive")
                    .session(session);

                if (!product || !product.isActive) {
                    throw createHttpError(
                        404,
                        "Product not found or inactive"
                    );
                }

                throw createHttpError(
                    409,
                    "Insufficient available stock"
                );
            }

            const inventorySnapshot = {
                availableStock: updatedProduct.availableStock,
                reservedStock: updatedProduct.reservedStock,
                soldStock: updatedProduct.soldStock,
            };

            const configuredTtlMs = Number(
                process.env.RESERVATION_TTL_MS
            );

            const reservationTtlMs =
                Number.isFinite(configuredTtlMs) &&
                configuredTtlMs > 0
                    ? configuredTtlMs
                    : 15 * 60 * 1000;

            const expiresAt = new Date(
                Date.now() + reservationTtlMs
            );

            const prepareStartedAt = performance.now();
            const now = new Date();

            // Construct the reservation document.
            // Mongoose supplies schema casting and defaults.
            const reservationDoc = new Reservation({
                product: updatedProduct._id,
                quantity,
                status: "pending",
                expiresAt,
                idempotencyKey,
                inventorySnapshot,
                createdAt: now,
                updatedAt: now,
                __v: 0,
            });

            // Construct the corresponding outbox event.
            const outboxDoc = new OutboxEvent({
                eventType: "reservation.expiry.schedule",
                aggregateType: "Reservation",
                aggregateId: reservationDoc._id,

                dedupeKey: `reservation-expiry-${reservationDoc._id}`,

                payload: {
                    reservationId: String(reservationDoc._id),
                    expiresAt: reservationDoc.expiresAt,
                },

                status: "pending",
                createdAt: now,
                updatedAt: now,
                __v: 0,
            });

            // Run Mongoose schema validation before using the native driver.
            await reservationDoc.validate();
            await outboxDoc.validate();

            // Convert the validated documents into plain BSON-compatible objects.
            const reservationToInsert = reservationDoc.toObject({
                depopulate: true,
                virtuals: false,
            });

            const outboxToInsert = outboxDoc.toObject({
                depopulate: true,
                virtuals: false,
            });

            reservationOutboxPrepareMs =
                performance.now() - prepareStartedAt;

            // Insert both documents in one client-level bulk-write request.
            const bulkWriteStartedAt = performance.now();

            const dbName = mongoose.connection.db.databaseName;

            const bulkResult = await mongoose.connection.client.bulkWrite(
                [
                    {
                        namespace: `${dbName}.${Reservation.collection.name}`,
                        name: "insertOne",
                        document: reservationToInsert,
                    },
                    {
                        namespace: `${dbName}.${OutboxEvent.collection.name}`,
                        name: "insertOne",
                        document: outboxToInsert,
                    },
                ],
                {
                    session,
                    ordered: true,
                }
            );

            reservationOutboxBulkWriteMs =
                performance.now() - bulkWriteStartedAt;

            if (bulkResult.insertedCount !== 2) {
                throw new Error(
                    "Expected both reservation and outbox event to be inserted"
                );
            }

            // Keep using the Mongoose reservation document for the API response.
            const reservation = reservationDoc;

            result = {
                reservation: serializeReservation(reservation),
                inventory: inventorySnapshot,
                replayed: false,
            };
            transactionCallbackMs +=
                performance.now() - transactionCallbackStartedAt;

        });

        transactionMs = performance.now() - transactionStartedAt;

        if (!result.replayed) {
            const cacheStartedAt = performance.now();

            await invalidateProductListCache();

            cacheInvalidationMs =
                performance.now() - cacheStartedAt;
        }
        if (logReservationPerformance) {
            console.log("[reservation-performance]", {
                productUpdateMs: Number(productUpdateMs.toFixed(2)),
                reservationOutboxPrepareMs:
                    Number(reservationOutboxPrepareMs.toFixed(2)),
                reservationOutboxBulkWriteMs:
                    Number(reservationOutboxBulkWriteMs.toFixed(2)),
                transactionMs: Number(transactionMs.toFixed(2)),
                cacheInvalidationMs:
                    Number(cacheInvalidationMs.toFixed(2)),
                totalMs: Number(
                    (performance.now() - requestStartedAt).toFixed(2)
                ),
                sessionStartMs: Number(sessionStartMs.toFixed(2)),
                readyStateBeforeSession,
                readyStateAfterSession,
                transactionCallbackMs: Number(
                    transactionCallbackMs.toFixed(2)
                ),
                transactionOverheadMs: Number(
                    Math.max(0, transactionMs - transactionCallbackMs).toFixed(2)
                ),
            });
        }

        return result;
    } catch (error) {
        // Two identical requests can race past the initial lookup.
        // The unique index ensures that only one can create the record.
        const bulkWriteErrors = error?.writeErrors;

        const duplicateKeyInBulk =
            bulkWriteErrors instanceof Map
                ? [...bulkWriteErrors.values()].some(
                    (item) => item?.code === 11000
                )
                : Array.isArray(bulkWriteErrors)
                ? bulkWriteErrors.some(
                        (item) =>
                            item?.code === 11000 ||
                            item?.err?.code === 11000
                    )
                : false;

        const isDuplicateKey =
            error?.code === 11000 ||
            error?.errorResponse?.code === 11000 ||
            error?.err?.code === 11000 ||
            duplicateKeyInBulk;

        if (isDuplicateKey) {
            const existingReservation = await Reservation.findOne({
                idempotencyKey,
            }).lean();

            if (existingReservation) {
                return buildReplayResult(
                    existingReservation,
                    productId,
                    quantity
                );
            }
        }

        throw error;
    } finally {
        await session.endSession();
    }
};
const reserveInventory = async (args) => {
    const {
        productId,
        quantity,
        idempotencyKey,
    } = args;

    const key = String(idempotencyKey);
    const normalizedProductId = String(productId);

    const existingFlight = inFlightReservations.get(key);

    // Another identical request is already being processed.
    if (existingFlight) {
        if (
            existingFlight.productId !== normalizedProductId ||
            existingFlight.quantity !== quantity
        ) {
            throw createHttpError(
                409,
                "Idempotency-Key was already used with different reservation data"
            );
        }

        const result = await existingFlight.promise;

        return {
            ...result,
            replayed: true,
        };
    }

    // Start the database workflow once for this key.
    const promise = reserveInventoryInternal(args);

    inFlightReservations.set(key, {
        productId: normalizedProductId,
        quantity,
        promise,
    });

    try {
        return await promise;
    } finally {
        // Do not accidentally remove a newer request.
        const current = inFlightReservations.get(key);

        if (current?.promise === promise) {
            inFlightReservations.delete(key);
        }
    }
};
module.exports = {
    reserveInventory,
};