import autocannon from 'autocannon';

const baseUrl = process.env.BASE_URL ?? 'http://localhost:3000';
const target = `${baseUrl}/api/simulate?count=200&delayMs=0`;

console.log(`Running non-blocking load simulation against ${target}`);

const result = await autocannon({
  url: target,
  connections: Number(process.env.CONNECTIONS ?? 100),
  duration: Number(process.env.DURATION ?? 15),
  pipelining: 1,
  workers: Number(process.env.WORKERS ?? 2)
});

console.log(autocannon.printResult(result));
console.log('Interpretation: successful responses under concurrent streaming load show that requests are not blocked by a dedicated thread per stream.');
