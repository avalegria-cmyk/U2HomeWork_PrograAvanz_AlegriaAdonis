const baseUrl = process.env.BASE_URL ?? 'http://localhost:3000';

const eventTypes = ['orders.created', 'payments.authorized', 'inventory.updated'];

for (let index = 0; index < 25; index += 1) {
  const type = eventTypes[index % eventTypes.length];
  const response = await fetch(`${baseUrl}/api/events`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      type,
      payload: {
        orderId: `ORD-${String(index + 1).padStart(4, '0')}`,
        amount: Number((20 + Math.random() * 180).toFixed(2))
      }
    })
  });

  if (!response.ok) {
    throw new Error(`Seed request failed with ${response.status}`);
  }
}

console.log(`Seeded 25 reactive events into ${baseUrl}`);
