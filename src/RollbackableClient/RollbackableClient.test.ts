import RollbackableClient from './RollbackableClient';
import RollbackError from './Errors/RollbackError';

class TestClient extends RollbackableClient {
  public addAction(action: () => Promise<unknown>): void {
    this.rollbackActions.push(action);
  }

  public async closeTransaction(): Promise<void> {
    return Promise.resolve();
  }
}

describe('RollbackableClient', () => {
  it('should run rollback actions in reverse order', async () => {
    const client = new TestClient('test');
    const order: number[] = [];
    client.addAction(async () => {
      order.push(1);
    });
    client.addAction(async () => {
      order.push(2);
    });
    client.addAction(async () => {
      order.push(3);
    });

    await client.rollback();

    expect(order).toEqual([3, 2, 1]);
  });

  it('should continue rolling back when an action fails and aggregate the failures', async () => {
    const client = new TestClient('test');
    const ran: string[] = [];
    client.addAction(async () => {
      ran.push('first');
    });
    client.addAction(async () => {
      ran.push('second');
      throw new Error('second failed');
    });
    client.addAction(async () => {
      ran.push('third');
    });

    await expect(client.rollback()).rejects.toThrow(RollbackError);

    expect(ran).toEqual(['third', 'second', 'first']);
  });

  it('should throw a RollbackError carrying the individual failures', async () => {
    const client = new TestClient('test');
    const failureA = new Error('a failed');
    const failureB = new Error('b failed');
    client.addAction(async () => {
      throw failureA;
    });
    client.addAction(async () => {
      throw failureB;
    });

    const error: RollbackError = await client.rollback().then(
      () => {
        throw new Error('rollback should have rejected');
      },
      (caught: unknown) => caught as RollbackError
    );

    expect(error.name).toBe('RollbackError');
    expect(error.rollbackFailures).toEqual([failureB, failureA]);
  });

  it('should be single-shot: a second rollback does not re-run actions', async () => {
    const client = new TestClient('test');
    let runs = 0;
    client.addAction(async () => {
      runs += 1;
    });

    await client.rollback();
    await client.rollback();

    expect(runs).toBe(1);
  });
});
