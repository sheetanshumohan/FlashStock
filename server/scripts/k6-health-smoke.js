import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
    vus: 1,
    iterations: 10,

    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<1000"],
    },
};

export default function () {
    const response = http.get(
        "http://localhost:5000/health/live"
    );

    check(response, {
        "status is 200": (r) => r.status === 200,
        "response is under 1 second": (r) =>
            r.timings.duration < 1000,
    });

    sleep(0.2);
}