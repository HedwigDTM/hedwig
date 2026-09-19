# Custom actions

Any side effect — an HTTP call, a database write, a third-party API — can
participate in a hedwig transaction by implementing `ICustomAction`:
`execute` runs inside the transaction, and `rollback` is recorded as its
compensation. If the transaction fails, compensations run in **reverse
registration order**, with the same resilience as every other hedwig client
(a failing compensation never blocks the remaining ones; failures aggregate
into a `RollbackError`).

## Example: a non-idempotent API call

```ts
import { TransactionManager, ICustomAction } from '@hedwig-team/dtm';

class CreateUserAction implements ICustomAction {
  constructor(
    private readonly api: ApiClient,
    private readonly email: string
  ) {}

  name = 'create-user';

  async execute(): Promise<unknown> {
    const { id } = await this.api.createUser(this.email);
    return id;
  }

  async rollback(): Promise<unknown> {
    // Compensate: delete the user that was created
    return this.api.deleteUser(this.email);
  }
}

const result = await manager.transaction(
  async ({ RedisClient, customActions }) => {
    const userId = await customActions.register(
      new CreateUserAction(api, 'ada@example.com')
    );

    await RedisClient.set('user:ada@example.com', String(result));
    return result;
  }
);
```

If anything after `register` throws, `rollback()` deletes the user again;
if `execute` itself fails, no compensation is recorded (nothing was done).

## Semantics

- `register` awaits `execute` and returns its result.
- A failing `execute` throws immediately and its `rollback` is **not**
  recorded.
- Compensations run in reverse registration order on rollback; individual
  failures are aggregated into `RollbackError.rollbackFailures`.
