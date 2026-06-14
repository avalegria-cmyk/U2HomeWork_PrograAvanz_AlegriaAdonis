import Fastify from 'fastify';
import { firstValueFrom, lastValueFrom } from 'rxjs';
import { defaultIfEmpty, map } from 'rxjs/operators';
import { createEventBus } from './event-bus.js';
import { writeObservableAsNdjson } from './rx-http.js';

export function buildServer({ logger = true, eventBus = createEventBus() } = {}) {
  const app = Fastify({ logger });

  app.decorate('eventBus', eventBus);

  app.get('/api/health', async () => {
    return firstValueFrom(
      eventBus.snapshotMetrics$().pipe(
        map((metrics) => ({
          status: 'ok',
          service: 'reactive-event-backend',
          nonBlocking: true,
          metrics
        }))
      )
    );
  });

  app.post('/api/events', async (request, reply) => {
    const body = request.body ?? {};
    const result$ = eventBus.snapshotMetrics$().pipe(
      map(() => eventBus.publish({ type: body.type, payload: body.payload }))
    );

    reply.code(201);
    return firstValueFrom(result$);
  });

  app.get('/api/events', async (request, reply) => {
    const abortController = new AbortController();
    request.raw.on('close', () => abortController.abort());
    reply.hijack();
    await writeObservableAsNdjson({
      observable: eventBus.history$(request.query.limit),
      response: reply.raw,
      abortSignal: abortController.signal,
      onBackpressure: () => eventBus.recordBackpressureWait()
    });
  });

  app.get('/api/events/live', async (request, reply) => {
    const abortController = new AbortController();
    request.raw.on('close', () => abortController.abort());
    reply.hijack();
    await writeObservableAsNdjson({
      observable: eventBus.live$(request.query.type),
      response: reply.raw,
      abortSignal: abortController.signal,
      onBackpressure: () => eventBus.recordBackpressureWait()
    });
  });

  app.get('/api/simulate', async (request, reply) => {
    const abortController = new AbortController();
    request.raw.on('close', () => abortController.abort());
    reply.hijack();
    await writeObservableAsNdjson({
      observable: eventBus.simulation$(request.query.count, request.query.delayMs),
      response: reply.raw,
      abortSignal: abortController.signal,
      onBackpressure: () => eventBus.recordBackpressureWait()
    });
  });

  app.get('/api/analytics', async () => {
    return lastValueFrom(
      eventBus.analytics$().pipe(
        defaultIfEmpty({
          total: 0,
          byType: {},
          lastEventAt: null
        }),
        map((summary) => ({
          generatedBy: 'RxJS scan over event history stream',
          summary
        }))
      )
    );
  });

  return app;
}
