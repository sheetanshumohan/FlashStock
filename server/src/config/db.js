const mongoose = require("mongoose");

const monitoredCommands = new Set([
    "findAndModify",
    "bulkWrite",
    "commitTransaction",
    "abortTransaction",
]);

const connectDB = async () => {
    if (!process.env.MONGO_URI) {
        throw new Error("MONGO_URI is missing from environment variables");
    }

    const monitorCommands =
    process.env.MONGO_COMMAND_MONITORING === "true";

    const connectionOptions = {
        monitorCommands,
    };

    // Optional override for isolated integration tests.
    if (process.env.MONGO_DB_NAME) {
        connectionOptions.dbName = process.env.MONGO_DB_NAME;
    }

    await mongoose.connect(
        process.env.MONGO_URI,
        connectionOptions
    );

    if (monitorCommands) {
        const client = mongoose.connection.getClient();

        client.on("commandSucceeded", (event) => {
            if (!monitoredCommands.has(event.commandName)) {
                return;
            }

            console.log("[mongo-command-performance]", {
                commandName: event.commandName,
                durationMs: Number(event.duration.toFixed(2)),
            });
        });

        client.on("commandFailed", (event) => {
            if (!monitoredCommands.has(event.commandName)) {
                return;
            }

            console.log("[mongo-command-failed]", {
                commandName: event.commandName,
                durationMs: Number(event.duration.toFixed(2)),
            });
        });
    }
    const monitorPool =
    process.env.MONGO_POOL_MONITORING === "true";

    if (monitorPool) {
        const client = mongoose.connection.getClient();

        client.on("connectionCheckedOut", (event) => {
            if (event.durationMS >= 5) {
                console.log("[mongo-pool-checkout]", {
                    durationMs: Number(event.durationMS.toFixed(2)),
                });
            }
        });

        client.on("connectionCheckOutFailed", (event) => {
            console.log("[mongo-pool-checkout-failed]", {
                durationMs: Number(event.durationMS.toFixed(2)),
                reason: event.reason,
            });
        });
    }

    console.log("MongoDB connected successfully");
};

module.exports = connectDB;