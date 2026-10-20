const { Queue } = require("bullmq");

const {
    getQueueConnectionOptions,
} = require("../config/bullmq-connection");

const reservationExpiryQueue = new Queue(
    "flashstock-reservation-expiry",
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

reservationExpiryQueue.on("error", (error) => {
    console.error(
        "Reservation queue error:",
        error.message
    );
});


const scheduleReservationExpiry = async ({
    reservationId,
    expiresAt,
    outboxEventId,
}) => {
    const expiryTime = new Date(expiresAt).getTime();

    if (!Number.isFinite(expiryTime)) {
        throw new Error("Invalid reservation expiry time");
    }

    const delay = Math.max(0, expiryTime - Date.now());

    return reservationExpiryQueue.add(
        "expire-reservation",
        {
            reservationId: String(reservationId),
            outboxEventId: String(outboxEventId),
        },
        {
            jobId: `reservation-expiry-${reservationId}`,
            delay,
        }
    );
};


module.exports = {
    reservationExpiryQueue,
    scheduleReservationExpiry,
};