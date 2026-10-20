const path = require("node:path");

require("dotenv").config({
    path: path.resolve(__dirname, "../../.env"),
});

const test = require("node:test");
const assert = require("node:assert/strict");
const mongoose = require("mongoose");
const { spawn } = require("node:child_process");
const { setTimeout: sleep } = require("node:timers/promises");
const { Queue } = require("bullmq");

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
    getQueueConnectionOptions,
} = require("../../src/config/bullmq-connection");

const TEST_DB_NAME = "flashstock_test";

test(
    "actual order worker recovers an abandoned processing order",
    async () => {
        assert.ok(
            process.env.MONGO_URI,
            "MONGO_URI must be configured in .env"
        );

        const queueName =
            `flashstock-order-recovery-test-${process.pid}-${Date.now()}`;

        let connected = false;
        let productId = null;
        let reservationId = null;
        let orderId = null;

        let queue;
        let child;
        let childOutput = "";
        let childErrors = "";

        const waitForChildMarker = async (
            marker,
            timeoutMs = 15000
        ) => {
            const deadline = Date.now() + timeoutMs;

            while (Date.now() < deadline) {
                if (childOutput.includes(marker)) {
                    return;
                }

                if (
                    child.exitCode !== null ||
                    child.signalCode !== null
                ) {
                    throw new Error(
                        `Worker exited before "${marker}". ` +
                        `stdout: ${childOutput}; ` +
                        `stderr: ${childErrors}`
                    );
                }

                await sleep(50);
            }

            throw new Error(
                `Timed out waiting for "${marker}". ` +
                `stdout: ${childOutput}; ` +
                `stderr: ${childErrors}`
            );
        };

        const stopChild = async () => {
            if (
                !child ||
                child.exitCode !== null ||
                child.signalCode !== null
            ) {
                return;
            }

            const exited = new Promise((resolve) => {
                child.once("exit", resolve);
            });

            child.kill("SIGTERM");

            const graceful = await Promise.race([
                exited.then(() => true),
                sleep(5000).then(() => false),
            ]);

            if (!graceful) {
                const forceExit = new Promise((resolve) => {
                    if (
                        child.exitCode !== null ||
                        child.signalCode !== null
                    ) {
                        resolve();
                    } else {
                        child.once("exit", resolve);
                    }
                });

                child.kill("SIGKILL");
                await forceExit;
            }
        };

        try {
            // The parent test process uses a dedicated database.
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
                name: "Actual Worker Recovery Test Product",
                sku: `TEST_WORKER_RECOVERY_${Date.now()}`,
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
                    `test-worker-reservation-${Date.now()}`,
            });

            reservationId = reservationResult.reservation.id;

            const orderResult = await createOrder({
                reservationId,
                idempotencyKey:
                    `test-worker-order-${Date.now()}`,
                testPaymentOutcome: "success",
            });

            orderId = orderResult.order.id;

            // Simulate a worker that claimed an order but stopped
            // before finishing. The attempt count records that claim.
            const staleTimestamp = new Date(Date.now() - 5000);

            const simulatedClaim = await Order.updateOne(
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

            assert.equal(simulatedClaim.modifiedCount, 1);

            // Queue used only by this test, not the live order queue.
            queue = new Queue(queueName, {
                connection: getQueueConnectionOptions(),
            });

            await queue.waitUntilReady();

            // Start the actual application worker in a child process.
            child = spawn(
                process.execPath,
                ["src/workers/order-processing.worker.js"],
                {
                    cwd: process.cwd(),
                    env: {
                        ...process.env,
                        MONGO_DB_NAME: TEST_DB_NAME,
                        ORDER_PROCESSING_QUEUE_NAME: queueName,
                        ORDER_PROCESSING_LEASE_MS: "1000",
                    },
                    stdio: ["ignore", "pipe", "pipe"],
                }
            );

            child.stdout.on("data", (chunk) => {
                childOutput += chunk.toString();
            });

            child.stderr.on("data", (chunk) => {
                childErrors += chunk.toString();
            });

            await waitForChildMarker(
                "[order-worker] Worker started"
            );

            const jobId = `test-order-process-${orderId}`;

            await queue.add(
                "process-order",
                {
                    orderId: String(orderId),
                    // No outboxEventId is intentional for this test.
                },
                {
                    jobId,
                    attempts: 3,
                    backoff: {
                        type: "exponential",
                        delay: 100,
                    },
                    removeOnComplete: false,
                    removeOnFail: false,
                }
            );

            // Wait until the actual worker completes the queued job.
            const deadline = Date.now() + 20000;
            let finalState = "unknown";

            while (Date.now() < deadline) {
                const job = await queue.getJob(jobId);

                if (job) {
                    finalState = await job.getState();

                    if (finalState === "completed") {
                        break;
                    }

                    if (finalState === "failed") {
                        throw new Error(
                            `Order worker job failed. ` +
                            `stdout: ${childOutput}; ` +
                            `stderr: ${childErrors}`
                        );
                    }
                }

                if (
                    child.exitCode !== null ||
                    child.signalCode !== null
                ) {
                    throw new Error(
                        `Order worker exited unexpectedly. ` +
                        `stdout: ${childOutput}; ` +
                        `stderr: ${childErrors}`
                    );
                }

                await sleep(100);
            }

            assert.equal(
                finalState,
                "completed",
                `Timed out waiting for worker completion. ` +
                `stdout: ${childOutput}; stderr: ${childErrors}`
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

            assert.equal(savedReservation.status, "confirmed");

            const savedProduct = await Product.findById(
                productId
            ).lean();

            assert.equal(savedProduct.availableStock, 4);
            assert.equal(savedProduct.reservedStock, 0);
            assert.equal(savedProduct.soldStock, 1);

            const completedJob = await queue.getJob(jobId);

            assert.ok(completedJob);

            console.log("Actual order-worker recovery test passed:", {
                queueName,
                jobState: await completedJob.getState(),
                orderStatus: savedOrder.status,
                paymentAttempts: savedOrder.paymentAttempts,
                reservationStatus: savedReservation.status,
                availableStock: savedProduct.availableStock,
                reservedStock: savedProduct.reservedStock,
                soldStock: savedProduct.soldStock,
            });
        } finally {
            try {
                // Stop the child before cleaning up its database records.
                await stopChild();

                if (queue) {
                    try {
                        await queue.obliterate({ force: true });
                    } finally {
                        await queue.close();
                    }
                }

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
                        await Order.deleteOne({ _id: orderId });
                    }

                    if (reservationId) {
                        await Reservation.deleteOne({
                            _id: reservationId,
                        });
                    }

                    if (productId) {
                        await Product.deleteOne({ _id: productId });
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