const express = require("express");

const {
    confirmReservation,
    releaseReservation,
} = require("../controllers/reservation.controller");

const {
    createRateLimiter,
} = require("../middleware/rateLimiter");

const reservationActionLimiter = createRateLimiter({
    windowMs: 60_000,
    max: 30,
    keyPrefix: "flashstock:ratelimit:reservation-actions",
    failClosed: true,
});
const router = express.Router();

router.post("/:reservationId/confirm", reservationActionLimiter, confirmReservation);
router.post("/:reservationId/release", reservationActionLimiter, releaseReservation);

module.exports = router;