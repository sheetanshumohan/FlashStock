
const path = require("path");

require("dotenv").config({
    path: path.resolve(__dirname, "../.env"),
});

const mongoose = require("mongoose");

const app = require("./app");
const connectDB = require("./config/db");

const {
    connectRedis,
    redisClient,
} = require("./config/redis");

const PORT = process.env.PORT || 5000;

let server;
let shuttingDown = false;

const SHUTDOWN_DRAIN_MS = 3000;

const shutdown = (signal) => {
    if (shuttingDown) {
        return;
    }

    shuttingDown = true;

    // Readiness changes immediately.
    app.locals.shuttingDown = true;

    console.log(
        `[server] Received ${signal}; readiness disabled`
    );

    console.log(
        `[server] Draining for ${SHUTDOWN_DRAIN_MS}ms`
    );

    // Give load balancers a short window to detect
    // that this instance is no longer ready.
    setTimeout(() => {
        if (!server) {
            process.exitCode = 1;
            return;
        }

        // Stop accepting new connections and wait for
        // existing HTTP requests to finish.
        server.close(async (serverError) => {
            if (serverError) {
                console.error(
                    "[server] HTTP shutdown error:",
                    serverError.message
                );
            }

            try {
                if (redisClient.isOpen) {
                    await redisClient.quit();
                    console.log("[server] Redis disconnected");
                }

                await mongoose.disconnect();
                console.log("[server] MongoDB disconnected");

                console.log("[server] Shutdown complete");

                process.exitCode = serverError ? 1 : 0;
            } catch (error) {
                console.error(
                    "[server] Shutdown failed:",
                    error.message
                );

                process.exitCode = 1;
            }
        });
    }, SHUTDOWN_DRAIN_MS);
};

const startServer = async () => {
    try {
        await connectDB();
        await connectRedis();

        server = app.listen(PORT, () => {
            console.log(
                `FlashStock server running on port ${PORT}`
            );
        });

        process.once("SIGINT", () => shutdown("SIGINT"));
        process.once("SIGTERM", () => shutdown("SIGTERM"));
    } catch (error) {
        console.error(
            "[server] Startup failed:",
            error.message
        );

        process.exitCode = 1;
    }
};

startServer();
