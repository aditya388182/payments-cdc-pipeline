import React, { useState } from "react";
import { ScrollView, View } from "react-native";
import { ApiError } from "@/api/client";
import type { Alert } from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { fmtTime } from "@/components/formatting";
import { Body, Button, Card, EmptyState, ErrorState, Heading, Loading, Pill, Screen, type Tone } from "@/components/ui";
import { useAsync } from "@/hooks/useAsync";
import { space } from "@/theme/theme";

const TONE: Record<string, Tone> = { critical: "bad", warning: "warn" };

function AlertCard({ a, onAck, busy }: { a: Alert; onAck?: () => void; busy: boolean }): React.JSX.Element {
  return (
    <Card>
      <View testID={`alert-${String(a.id)}`}>
        <View style={{ flexDirection: "row", gap: 8, marginBottom: space.sm }}>
          <Pill label={a.state.toUpperCase()} tone={a.state === "firing" ? "bad" : a.state === "acknowledged" ? "warn" : "good"} />
          <Pill label={a.severity} tone={TONE[a.severity] ?? "info"} />
        </View>
        <Heading>{a.rule.replace(/_/g, " ")}</Heading>
        <Body>{a.summary}</Body>
        <Body muted>Fired {fmtTime(a.fired_at)}{a.acked_by ? ` · acknowledged by ${a.acked_by}` : ""}{a.resolved_at ? ` · resolved ${fmtTime(a.resolved_at)}` : ""}</Body>
        {onAck ? (
          <View style={{ marginTop: space.md }}>
            <Button label="Acknowledge" onPress={onAck} disabled={busy} testID={`ack-${String(a.id)}`} accessibilityHint="Marks this alert as acknowledged and records it in the audit log" />
          </View>
        ) : null}
      </View>
    </Card>
  );
}

export function AlertsScreen(): React.JSX.Element {
  const { api } = useAuth();
  const q = useAsync(() => api.alerts(), [api], 5000);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const ack = async (id: number): Promise<void> => {
    setBusy(true);
    setErr(null);
    try {
      await api.ackAlert(id);
      q.reload();
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Could not acknowledge.");
    } finally {
      setBusy(false);
    }
  };

  if (q.loading && !q.data) return <Screen><Loading label="Loading alerts" /></Screen>;
  if (q.error && !q.data) return <Screen><ErrorState message={q.error} onRetry={q.reload} /></Screen>;
  const alerts = q.data?.alerts ?? [];
  const groups: { title: string; state: Alert["state"] }[] = [
    { title: "Firing", state: "firing" },
    { title: "Acknowledged", state: "acknowledged" },
    { title: "Resolved", state: "resolved" },
  ];
  if (alerts.length === 0) return <Screen><EmptyState title="No alerts" hint="Alerts appear here when lag, freshness, the connector, the DLQ or parity cross a threshold." /></Screen>;
  return (
    <Screen>
      <ScrollView contentContainerStyle={{ padding: space.lg }} testID="alerts-scroll">
        {err ? <ErrorState message={err} /> : null}
        {groups.map((g) => {
          const items = alerts.filter((a) => a.state === g.state);
          return (
            <View key={g.state} testID={`alerts-${g.state}`}>
              <Heading style={{ marginBottom: space.sm }}>{g.title} ({items.length})</Heading>
              {items.length === 0 ? <Body muted style={{ marginBottom: space.lg }}>None</Body> : items.map((a) => <AlertCard key={a.id} a={a} busy={busy} onAck={g.state === "firing" ? () => void ack(a.id) : undefined} />)}
            </View>
          );
        })}
      </ScrollView>
    </Screen>
  );
}
