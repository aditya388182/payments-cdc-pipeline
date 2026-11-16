import React, { useCallback, useState } from "react";
import { FlatList } from "react-native";
import type { AuditEntry } from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { fmtTime } from "@/components/formatting";
import { Body, Button, Card, EmptyState, ErrorState, Heading, Loading, Pill, Screen } from "@/components/ui";
import { useDeferredEffect } from "@/hooks/useDeferredEffect";
import { space } from "@/theme/theme";

export function AuditScreen(): React.JSX.Element {
  const { api, role } = useAuth();
  const [entries, setEntries] = useState<AuditEntry[]>([]);
  const [next, setNext] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (cursor?: string): Promise<void> => {
    setLoading(true);
    setError(null);
    try {
      const page = await api.audit(cursor);
      setEntries((cur) => (cursor ? [...cur, ...page.entries] : page.entries));
      setNext(page.next_cursor);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load the audit log.");
    } finally {
      setLoading(false);
    }
  }, [api]);

  useDeferredEffect(() => {
    if (role === "admin") void load();
  }, [role, load]);

  if (role !== "admin") return <Screen><EmptyState title="Admins only" hint="The audit log is visible to admins." /></Screen>;
  if (loading && entries.length === 0) return <Screen><Loading label="Loading the audit log" /></Screen>;
  if (error && entries.length === 0) return <Screen><ErrorState message={error} onRetry={() => void load()} /></Screen>;
  if (entries.length === 0) return <Screen><EmptyState title="No audit entries yet" hint="Replays, acknowledgements and generator changes are recorded here." /></Screen>;
  return (
    <Screen>
      <FlatList
        testID="audit-list"
        data={entries}
        keyExtractor={(e) => String(e.id)}
        contentContainerStyle={{ padding: space.lg }}
        renderItem={({ item }) => (
          <Card>
            <Pill label={item.result} tone={item.result === "error" ? "bad" : item.result === "ok" || item.result === "APPLIED" || item.result === "ALREADY_APPLIED" ? "good" : "warn"} />
            <Heading style={{ marginTop: space.sm }}>{item.action}</Heading>
            <Body>{item.actor} ({item.role}) → {item.target || "—"}</Body>
            <Body muted>{fmtTime(item.requested_at)}{item.error ? ` · ${item.error}` : ""}</Body>
          </Card>
        )}
        ListFooterComponent={next ? <Button label="Load more" tone="plain" onPress={() => void load(next)} testID="audit-more" /> : undefined}
      />
    </Screen>
  );
}
