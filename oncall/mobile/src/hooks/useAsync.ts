import { useCallback, useEffect, useRef, useState } from "react";
import { useDeferredEffect } from "./useDeferredEffect";

export interface AsyncState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  reload: () => void;
}

/** Runs `fn` on mount and whenever `deps` change or `reload()` is called; ignores results of superseded runs. */
export function useAsync<T>(fn: () => Promise<T>, deps: readonly unknown[] = [], pollMs = 0): AsyncState<T> {
  const [state, setState] = useState<{ data: T | null; error: string | null; loading: boolean }>({ data: null, error: null, loading: true });
  const run = useRef(0);
  const fnRef = useRef(fn);
  useEffect(() => {
    fnRef.current = fn;
  });

  const load = useCallback((quiet: boolean) => {
    const id = ++run.current;
    if (!quiet) setState((s) => ({ ...s, loading: true, error: null }));
    fnRef.current().then(
      (data) => {
        if (id === run.current) setState({ data, error: null, loading: false });
      },
      (e: unknown) => {
        if (id === run.current) setState((s) => ({ data: quiet ? s.data : null, error: e instanceof Error ? e.message : "request failed", loading: false }));
      },
    );
  }, []);

  useDeferredEffect(() => load(false), deps);

  useEffect(() => {
    if (!pollMs) return;
    const t = setInterval(() => load(true), pollMs);
    return () => clearInterval(t);
  }, [pollMs, load]);

  const reload = useCallback(() => load(false), [load]);
  return { ...state, reload };
}
