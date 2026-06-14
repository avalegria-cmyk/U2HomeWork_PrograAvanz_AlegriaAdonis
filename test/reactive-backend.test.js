import assert from 'node:assert/strict';
import { test } from 'node:test';
import { buildServer } from '../src/app.js';

async function withServer(run) {
  const app = buildServer({ logger: false });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  const baseUrl = `http://127.0.0.1:${address.port}`;

  try {
    await run(baseUrl);
  } finally {
    await app.close();
  }
}

test('health endpoint exposes the reactive runtime status', async () => {
  await withServer(async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/health`);
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.equal(body.status, 'ok');
    assert.equal(body.nonBlocking, true);
    assert.equal(body.metrics.runtime, 'Fastify + RxJS Observables');
  });
});

test('events are published and then streamed from the event history', async () => {
  await withServer(async (baseUrl) => {
    const publish = await fetch(`${baseUrl}/api/events`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'orders.created',
        payload: { orderId: 'ORD-TEST-001' }
      })
    });
    const created = await publish.json();

    const stream = await fetch(`${baseUrl}/api/events?limit=1`);
    const lines = (await stream.text()).trim().split('\n');
    const streamed = JSON.parse(lines[0]);

    assert.equal(publish.status, 201);
    assert.equal(stream.status, 200);
    assert.equal(stream.headers.get('x-reactive-mode'), 'observable-stream');
    assert.equal(streamed.id, created.id);
    assert.equal(streamed.type, 'orders.created');
  });
});

test('simulation endpoint streams a finite observable sequence', async () => {
  await withServer(async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulate?count=7&delayMs=0`);
    const lines = (await response.text()).trim().split('\n');

    assert.equal(response.status, 200);
    assert.equal(lines.length, 7);
    assert.equal(JSON.parse(lines.at(-1)).sequence, 7);
  });
});

test('analytics endpoint reduces event history through an RxJS stream', async () => {
  await withServer(async (baseUrl) => {
    await fetch(`${baseUrl}/api/events`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'payments.authorized', payload: { amount: 10 } })
    });

    const response = await fetch(`${baseUrl}/api/analytics`);
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.equal(body.generatedBy, 'RxJS scan over event history stream');
    assert.equal(body.summary.total, 1);
    assert.equal(body.summary.byType['payments.authorized'], 1);
  });
});
