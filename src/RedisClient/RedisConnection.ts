/**
 * The minimal Redis surface hedwig uses. Deliberately structural: any
 * node-redis client (any module instantiation of RedisClientType) satisfies
 * it, so createClient's inferred generics never need a type assertion to
 * cross into this library.
 */
export interface RedisConnection {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<string | null>;
  del(key: string): Promise<number>;
  exists(key: string): Promise<number>;
  incr(key: string): Promise<number>;
  decr(key: string): Promise<number>;
  hGet(key: string, field: string): Promise<string | undefined>;
  hSet(key: string, field: string, value: string | number): Promise<number>;
}
