const {
    reservationIdParamsSchema,
} = require("../validators/reservation.validators");

const {
    confirmReservation: confirmReservationService,
    releaseReservation: releaseReservationService,
} = require("../services/reservation.service");

const parseReservationId = (req, res) => {
    const result = reservationIdParamsSchema.safeParse(
        req.params
    );

    if (!result.success) {
        res.status(400).json({
            success: false,
            message: "Invalid reservation ID",
        });

        return null;
    }

    return result.data.reservationId;
};

const confirmReservation = async (req, res, next) => {
    const reservationId = parseReservationId(req, res);

    if (!reservationId) return;

    try {
        const data = await confirmReservationService(
            reservationId
        );

        return res.status(200).json({
            success: true,
            message: data.replayed
                ? "Reservation was already confirmed"
                : "Reservation confirmed successfully",
            data,
        });
    } catch (error) {
        return next(error);
    }
};

const releaseReservation = async (req, res, next) => {
    const reservationId = parseReservationId(req, res);

    if (!reservationId) return;

    try {
        const data = await releaseReservationService(
            reservationId
        );

        return res.status(200).json({
            success: true,
            message: data.replayed
                ? "Reservation was already released or expired"
                : "Reservation released successfully",
            data,
        });
    } catch (error) {
        return next(error);
    }
};

module.exports = {
    confirmReservation,
    releaseReservation,
};