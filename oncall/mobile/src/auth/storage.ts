import { Platform } from "react-native";

/**
 * Token storage. Native: expo-secure-store (Keychain / Keystore). Web has no secure store, so the web build (used for
 * Playwright and demos only) keeps tokens in sessionStorage, which is cleared when the tab closes. The native module
 * is required lazily so the web bundle and Jest never load it.
 */
export interface KeyValueStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  remove(key: string): Promise<void>;
}

const memory = new Map<string, string>();

class WebStore implements KeyValueStore {
  get(key: string): Promise<string | null> {
    try {
      return Promise.resolve(globalThis.sessionStorage?.getItem(key) ?? memory.get(key) ?? null);
    } catch {
      return Promise.resolve(memory.get(key) ?? null);
    }
  }
  set(key: string, value: string): Promise<void> {
    memory.set(key, value);
    try {
      globalThis.sessionStorage?.setItem(key, value);
    } catch {
      /* storage blocked: the in-memory copy still works for this tab */
    }
    return Promise.resolve();
  }
  remove(key: string): Promise<void> {
    memory.delete(key);
    try {
      globalThis.sessionStorage?.removeItem(key);
    } catch {
      /* ignore */
    }
    return Promise.resolve();
  }
}

class NativeSecureStore implements KeyValueStore {
  private mod() {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return require("expo-secure-store") as typeof import("expo-secure-store");
  }
  get(key: string): Promise<string | null> {
    return this.mod().getItemAsync(key);
  }
  set(key: string, value: string): Promise<void> {
    return this.mod().setItemAsync(key, value);
  }
  remove(key: string): Promise<void> {
    return this.mod().deleteItemAsync(key);
  }
}

export class MemoryStore implements KeyValueStore {
  private m = new Map<string, string>();
  get(key: string): Promise<string | null> {
    return Promise.resolve(this.m.get(key) ?? null);
  }
  set(key: string, value: string): Promise<void> {
    this.m.set(key, value);
    return Promise.resolve();
  }
  remove(key: string): Promise<void> {
    this.m.delete(key);
    return Promise.resolve();
  }
}

export function defaultStore(): KeyValueStore {
  return Platform.OS === "web" ? new WebStore() : new NativeSecureStore();
}
