const getBaseConnectionOptions = () => {
    const redisUrl = new URL(
        process.env.REDIS_URL || "redis://127.0.0.1:6379"
    );

    const options = {
        host: redisUrl.hostname,
        port: Number(
            redisUrl.port ||
            (redisUrl.protocol === "rediss:" ? 6380 : 6379)
        ),
    };

    if (redisUrl.username) {
        options.username = decodeURIComponent(redisUrl.username);
    }

    if (redisUrl.password) {
        options.password = decodeURIComponent(redisUrl.password);
    }

    if (redisUrl.pathname.length > 1) {
        options.db = Number(redisUrl.pathname.slice(1));
    }

    if (redisUrl.protocol === "rediss:") {
        options.tls = {};
    }

    return options;
};

// API-side queue producer: fail relatively quickly if Redis is unavailable.
const getQueueConnectionOptions = () => ({
    ...getBaseConnectionOptions(),
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
});

// Worker-side connection: permit reconnection while jobs are being processed.
const getWorkerConnectionOptions = () => ({
    ...getBaseConnectionOptions(),
    maxRetriesPerRequest: null,
});

module.exports = {
    getQueueConnectionOptions,
    getWorkerConnectionOptions,
};