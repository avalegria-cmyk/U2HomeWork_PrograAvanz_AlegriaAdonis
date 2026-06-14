import { once } from 'node:events';

export function observableToAsyncIterable(observable, abortSignal) {
  return {
    [Symbol.asyncIterator]() {
      const values = [];
      const errors = [];
      let done = false;
      let wake;

      const notify = () => {
        if (wake) {
          wake();
          wake = undefined;
        }
      };

      const subscription = observable.subscribe({
        next(value) {
          values.push(value);
          notify();
        },
        error(error) {
          errors.push(error);
          done = true;
          notify();
        },
        complete() {
          done = true;
          notify();
        }
      });

      const abort = () => {
        done = true;
        subscription.unsubscribe();
        notify();
      };

      abortSignal?.addEventListener('abort', abort, { once: true });

      return {
        async next() {
          while (!values.length && !done && !errors.length) {
            await new Promise((resolve) => {
              wake = resolve;
            });
          }

          if (errors.length) {
            throw errors.shift();
          }

          if (values.length) {
            return { value: values.shift(), done: false };
          }

          abortSignal?.removeEventListener('abort', abort);
          subscription.unsubscribe();
          return { value: undefined, done: true };
        },
        async return() {
          abort();
          abortSignal?.removeEventListener('abort', abort);
          return { value: undefined, done: true };
        }
      };
    }
  };
}

export async function writeObservableAsNdjson({ observable, response, abortSignal, onBackpressure }) {
  response.setHeader('content-type', 'application/x-ndjson; charset=utf-8');
  response.setHeader('cache-control', 'no-cache');
  response.setHeader('x-reactive-mode', 'observable-stream');

  try {
    for await (const item of observableToAsyncIterable(observable, abortSignal)) {
      const canContinue = response.write(`${JSON.stringify(item)}\n`);
      if (!canContinue) {
        onBackpressure?.();
        await once(response, 'drain');
      }
    }
  } finally {
    if (!response.destroyed && !response.writableEnded) {
      response.end();
    }
  }
}
