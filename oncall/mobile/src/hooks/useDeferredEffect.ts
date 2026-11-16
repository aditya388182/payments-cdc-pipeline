import { useEffect, type DependencyList } from "react";

/**
 * Like useEffect, but runs `fn` in a microtask. Used for "load data when something changes" effects whose first
 * action is a setState (for example a loading flag): React's lint rules (and good practice) discourage setState
 * synchronously inside the effect body, and deferring by one microtask keeps renders from cascading.
 * The cleanup cancels a run that has not started yet.
 */
export function useDeferredEffect(fn: () => void | (() => void), deps: DependencyList): void {
  useEffect(() => {
    let cancelled = false;
    let cleanup: void | (() => void);
    queueMicrotask(() => {
      if (!cancelled) cleanup = fn();
    });
    return () => {
      cancelled = true;
      if (typeof cleanup === "function") cleanup();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
}
