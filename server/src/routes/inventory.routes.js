const express = require("express");

const {
    createReservation,
} = require("../controllers/inventory.controller");

const {
    createRateLimiter,
} = require("../middleware/rateLimiter");

const reservationLimiter = createRateLimiter({
    windowMs: 60_000,
    max: 30,
    keyPrefix: "flashstock:ratelimit:reservations",
    failClosed: true,
});
const router = express.Router();

router.post(
    "/:productId/reservations",
    reservationLimiter,
    createReservation
);

module.exports = router;