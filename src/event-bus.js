import { Subject, defer, from, interval, of } from 'rxjs';
import { filter, map, scan, take, tap } from 'rxjs/operators';

const MAX_HISTORY_SIZE = 1000;

export class ReactiveEventBus {
  constructor({ clock = () => new Date() } = {}) {
    this.clock = clock;
    this.events = [];
    this.publisher = new Subject();
    this.metrics = {
      published: 0,
      activeSubscribers: 0,
      completedStreams: 0,
      backpressureWaits: 0
    };
  }

  publish(input) {
    const event = {
      id: crypto.randomUUID(),
      type: input.type ?? 'generic',
      payload: input.payload ?? {},
      producedAt: this.clock().toISOString()
    };

    this.events.push(event);
    if (this.events.length > MAX_HISTORY_SIZE) {
      this.events.shift();
    }

    this.metrics.published += 1;
    this.publisher.next(event);
    return event;
  }

  history$(limit = 100) {
    return defer(() => {
      const safeLimit = Math.min(Math.max(Number(limit) || 100, 1), MAX_HISTORY_SIZE);
      return from(this.events.slice(-safeLimit));
    });
  }

  live$(type) {
    return this.publisher.asObservable().pipe(
      filter((event) => !type || event.type === type),
      tap({
        subscribe: () => {
          this.metrics.activeSubscribers += 1;
        },
        unsubscribe: () => {
          this.metrics.activeSubscribers = Math.max(0, this.metrics.activeSubscribers - 1);
        },
        finalize: () => {
          this.metrics.completedStreams += 1;
        }
      })
    );
  }

  simulation$(count = 100, delayMs = 5) {
    const safeCount = Math.min(Math.max(Number(count) || 100, 1), 10000);
    const safeDelay = Math.min(Math.max(Number(delayMs) || 1, 0), 1000);

    return interval(safeDelay).pipe(
      take(safeCount),
      map((index) => ({
        id: crypto.randomUUID(),
        type: 'simulation.tick',
        sequence: index + 1,
        payload: {
          cpuLoad: Number((35 + Math.random() * 45).toFixed(2)),
          queueDepth: Math.floor(Math.random() * 20)
        },
        producedAt: this.clock().toISOString()
      }))
    );
  }

  analytics$() {
    return this.history$(MAX_HISTORY_SIZE).pipe(
      scan(
        (summary, event) => {
          summary.total += 1;
          summary.byType[event.type] = (summary.byType[event.type] ?? 0) + 1;
          summary.lastEventAt = event.producedAt;
          return summary;
        },
        {
          total: 0,
          byType: {},
          lastEventAt: null
        }
      )
    );
  }

  snapshotMetrics$() {
    return of({
      ...this.metrics,
      storedEvents: this.events.length,
      runtime: 'Fastify + RxJS Observables',
      backpressureStrategy: 'Each HTTP writer awaits drain before pulling the next emitted item.'
    });
  }

  recordBackpressureWait() {
    this.metrics.backpressureWaits += 1;
  }
}

export function createEventBus(options) {
  return new ReactiveEventBus(options);
}
