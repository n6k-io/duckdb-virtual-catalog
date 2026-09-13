import { test, expect } from "bun:test";
import { createPushQueue } from "../workers/push-queue";

test("take returns only the requested catalog's events, in order, then drains", () => {
  const q = createPushQueue();
  q.enqueue("db", 12, { schemas: ["main"] });
  q.enqueue("other", 12, { schemas: ["x"] });
  q.enqueue("db", 12, { schemas: ["public"] });

  expect(JSON.parse(q.take("db"))).toEqual([
    { op: 12, body: '{"schemas":["main"]}' },
    { op: 12, body: '{"schemas":["public"]}' },
  ]);

  expect(q.take("db")).toBe("");
  expect(JSON.parse(q.take("other"))).toEqual([
    { op: 12, body: '{"schemas":["x"]}' },
  ]);
});

test("take on an unknown or already-drained catalog returns empty string", () => {
  const q = createPushQueue();
  expect(q.take("nope")).toBe("");
  q.enqueue("db", 1, null);
  expect(q.take("db")).not.toBe("");
  expect(q.take("db")).toBe("");
});
