import { RollbackError } from '../errors';
import { CustomActionRegistry } from './CustomActionRegistry';

describe('CustomActionRegistry', () => {
  it('register should run execute and return its result', async () => {
    const registry = new CustomActionRegistry('test');

    const result = await registry.register({
      execute: async () => 'executed',
      rollback: async () => undefined,
    });

    expect(result).toBe('executed');
  });

  it('rollback should run compensations in reverse registration order', async () => {
    const registry = new CustomActionRegistry('test');
    const order: string[] = [];

    await registry.register({
      name: 'first',
      execute: async () => undefined,
      rollback: async () => {
        order.push('first');
      },
    });
    await registry.register({
      name: 'second',
      execute: async () => undefined,
      rollback: async () => {
        order.push('second');
      },
    });

    await registry.rollback();

    expect(order).toEqual(['second', 'first']);
  });

  it('register should not record a compensation when execute fails', async () => {
    const registry = new CustomActionRegistry('test');
    let compensationRan = false;

    await expect(
      registry.register({
        name: 'failing action',
        execute: async () => {
          throw new Error('execute boom');
        },
        rollback: async () => {
          compensationRan = true;
        },
      })
    ).rejects.toThrow('Custom action "failing action" failed');

    await registry.rollback();
    expect(compensationRan).toBe(false);
  });

  it('rollback failures aggregate like any other hedwig client', async () => {
    const registry = new CustomActionRegistry('test');
    const ran: string[] = [];

    await registry.register({
      execute: async () => undefined,
      rollback: async () => {
        throw new Error('compensation failed');
      },
    });
    await registry.register({
      execute: async () => undefined,
      rollback: async () => {
        ran.push('second');
      },
    });

    await expect(registry.rollback()).rejects.toThrow(RollbackError);
    expect(ran).toEqual(['second']);
  });
});
