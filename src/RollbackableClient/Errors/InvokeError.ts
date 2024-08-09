/**
 * Custom error class for errors during client invocation.
 */
export default class InvocationError extends Error {
  constructor(message = "") {
    super(message);
    this.name = "RestoreError";
  }
}
