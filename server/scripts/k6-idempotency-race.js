import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const createdResponses = new Counter("created_responses");
const replayResponses = new Counter("replay_responses");
const unexpectedResponses = new Counter("unexpected_responses");

export const options = {
    scenarios: {
        duplicate_requests: {
            executor: "per-vu-iterations",
            vus: 20,
            iterations: 1,
            maxDuration: "30s",
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.01"],
        created_responses: ["count==1"],
        replay_responses: ["count==19"],
        unexpected_responses: ["count==0"],
    },
};

http.setResponseCallback(http.expectedStatuses(200, 201));

export default function () {
    const productId = __ENV.PRODUCT_ID;
    const idempotencyKey = __ENV.IDEMPOTENCY_KEY;

    if (!productId || !idempotencyKey) {
        throw new Error(
            "PRODUCT_ID and IDEMPOTENCY_KEY are required."
        );
    }

    const response = http.post(
        `http://localhost:5000/api/products/${productId}/reservations`,
        JSON.stringify({ quantity: 1 }),
        {
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": idempotencyKey,
            },
        }
    );

    let body = null;

    try {
        body = response.json();
    } catch (_) {
        // Unexpected response format is reported below.
    }

    if (
        response.status === 201 &&
        body?.success === true &&
        body?.result?.replayed === false
    ) {
        createdResponses.add(1);
    } else if (
        response.status === 200 &&
        body?.success === true &&
        body?.result?.replayed === true
    ) {
        replayResponses.add(1);
    } else {
        unexpectedResponses.add(1);

        console.log(
            `Unexpected response: HTTP ${response.status}, ` +
            `${String(response.body || "").slice(0, 200)}`
        );
    }

    check(response, {
        "request created or replayed a reservation": () =>
            (
                response.status === 201 &&
                body?.result?.replayed === false
            ) ||
            (
                response.status === 200 &&
                body?.result?.replayed === true
            ),
    });
}