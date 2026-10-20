const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const productRoutes = require("./routes/product.routes");
const inventoryRoutes = require("./routes/inventory.routes");
const reservationRoutes = require("./routes/reservation.routes");
const orderRoutes = require("./routes/order.routes");
const app = express();
app.locals.shuttingDown = false;
const mongoose = require("mongoose");
const {
    redisClient,
} = require("./config/redis");
// Add security-related HTTP headers
app.use(helmet());

// Parse JSON request bodies
app.use(express.json({ limit: "10kb" }));

// Allow cross-origin requests during development
app.use(cors());

// Liveness check
app.get("/health/live", (req, res) => {
    res.status(200).json({
        status: "ok",
    });
});

// Readiness check
app.get("/health/ready", (req, res) => {
    const checks = {
        mongodb: mongoose.connection.readyState === 1,
        redis: redisClient.isReady,
    };

    const shuttingDown = app.locals.shuttingDown === true;

    const ready =
        !shuttingDown &&
        Object.values(checks).every(Boolean);

    return res.status(ready ? 200 : 503).json({
        status: shuttingDown
            ? "shutting_down"
            : ready
                ? "ready"
                : "not_ready",
        checks,
    });
});
app.use((req, res, next) => {
    if (
        app.locals.shuttingDown &&
        !req.path.startsWith("/health/")
    ) {
        return res.status(503).json({
            success: false,
            message: "Server is shutting down. Try again later.",
        });
    }

    next();
});
app.use("/api/products", productRoutes);
app.use("/api/products", inventoryRoutes);
app.use("/api/reservations", reservationRoutes);
app.use("/api/orders", orderRoutes);

// Handle unknown routes
app.use((req, res) => {
    res.status(404).json({
        success: false,
        message: `Route not found: ${req.method} ${req.originalUrl}`,
    });
});

// Handle unexpected errors
app.use((err, req, res, next) => {
    console.error(err);

    if (res.headersSent) {
        return next(err);
    }

    const status = err.statusCode || err.status || 500;

    return res.status(status).json({
        success: false,
        message: status >= 500
            ? "Internal server error"
            : err.message,
    });
});


module.exports = app;