/**
 * Base class for all errors hedwig throws, so consumers can catch
 * `instanceof HedwigError` for any library-originated failure.
 */
export class HedwigError extends Error {
  constructor(message = '') {
    super(message);
    this.name = 'HedwigError';
  }
}
