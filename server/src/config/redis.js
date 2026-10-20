const { createClient } = require("redis");

const redisClient = createClient({
    url: process.env.REDIS_URL || "redis://127.0.0.1:6379",

    socket: {
        reconnectStrategy(retries) {
            return Math.min(retries * 100, 3000);
        },
    },
});

// Register the error handler before connecting.
redisClient.on("error", (error) => {
    console.error("Redis client error:", error.message);
});

redisClient.on("ready", () => {
    console.log("Redis connection ready");
});

redisClient.on("reconnecting", () => {
    console.log("Redis reconnecting...");
});

const connectRedis = async () => {
    if (!redisClient.isOpen) {
        await redisClient.connect();
    }

    if (!redisClient.isReady) {
        throw new Error("Redis connection is not ready");
    }

    console.log("Redis connected successfully");
};

module.exports = {
    redisClient,
    connectRedis,
};