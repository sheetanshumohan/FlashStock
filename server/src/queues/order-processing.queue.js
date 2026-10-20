const { Queue } = require("bullmq");

const {
    getQueueConnectionOptions,
} = require("../config/bullmq-connection");

const orderProcessingQueue = new Queue(
    "flashstock-order-processing",
    {
        connection: getQueueConnectionOptions(),

        defaultJobOptions: {
            attempts: 5,

            backoff: {
                type: "exponential",
                delay: 1000,
            },

            removeOnComplete: {
                count: 1000,
            },

            removeOnFail: {
                count: 5000,
            },
        },
    }
);

orderProcessingQueue.on("error", (error) => {
    console.error(
        "[order-queue] Redis queue error:",
        error.message
    );
});


const enqueueOrderProcessing = async (
    orderId,
    outboxEventId
) => {
    return orderProcessingQueue.add(
        "process-order",
        {
            orderId: String(orderId),
            ...(outboxEventId
                ? { outboxEventId: String(outboxEventId) }
                : {}),
        },
        {
            jobId: `order-process-${orderId}`,
        }
    );
};


module.exports = {
    orderProcessingQueue,
    enqueueOrderProcessing,
};