export function bindSharedNativeClient(Sdk) {
  const native = Sdk.withDefaultConfig();
  let bindings = 0;
  Object.defineProperty(Sdk, 'withDefaultConfig', {
    configurable: true,
    value() { bindings++; return native; },
  });
  return { native, get bindings() { return bindings; } };
}
