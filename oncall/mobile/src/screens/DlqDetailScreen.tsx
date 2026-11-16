import { useLocalSearchParams } from "expo-router";
import React from "react";
import { ScrollView, StyleSheet, View } from "react-native";
import { useAuth } from "@/auth/AuthContext";
import { fmtMoney, fmtTime } from "@/components/formatting";
import { ReplayPanel } from "@/components/ReplayPanel";
import { Body, Card, ErrorState, Heading, Loading, Pill, Screen } from "@/components/ui";
import { useAsync } from "@/hooks/useAsync";
import { space } from "@/theme/theme";

function pretty(json?: string): string {
  if (!json) return "";
  try {
    return JSON.stringify(JSON.parse(json), null, 2);
  } catch {
    return json;
  }
}

export function DlqDetailScreen(): React.JSX.Element {
  const { id } = useLocalSearchParams<{ id: string }>();
  const { api, role } = useAuth();
  const q = useAsync(() => api.dlqRecord(String(id)), [api, id]);
  if (q.loading) return <Screen><Loading label="Loading record" /></Screen>;
  if (q.error || !q.data) return <Screen><ErrorState message={q.error ?? "Record not found"} onRetry={q.reload} /></Screen>;
  const r = q.data;
  return (
    <Screen>
      <ScrollView contentContainerStyle={{ padding: space.lg }} testID="dlq-detail">
        <Card>
          <Pill label={r.rejection_reason.replace(/_/g, " ")} tone="bad" testID="detail-reason" />
          <Heading style={{ marginTop: space.sm }} >Validation error</Heading>
          <Body testID="detail-error">{r.rejection_reason} — the record was quarantined and never merged into the lake.</Body>
        </Card>
        <Card>
          <Row k="Transaction" v={r.transaction_id} />
          <Row k="Merchant" v={r.merchant_id} />
          <Row k="Amount" v={fmtMoney(r.amount_minor, r.currency)} />
          <Row k="Status" v={`${r.status} · ${r.event_type} · op ${r.op}`} />
          <Row k="LSN" v={String(r.lsn)} />
          <Row k="Rejected" v={fmtTime(r.rejected_at)} />
          <Row k="In the lake" v={r.applied ? "yes, already applied" : "no"} />
        </Card>
        <Card>
          <Heading style={{ marginBottom: space.sm }}>Payload</Heading>
          <Body testID="detail-payload" style={styles.mono}>{pretty(r.payload_json)}</Body>
        </Card>
        <ReplayPanel record={r} role={role} api={api} onResult={() => undefined} />
      </ScrollView>
    </Screen>
  );
}

function Row({ k, v }: { k: string; v: string }): React.JSX.Element {
  return (
    <View style={styles.row} accessible accessibilityLabel={`${k}: ${v}`}>
      <Body muted style={styles.key}>{k}</Body>
      <Body style={styles.val}>{v}</Body>
    </View>
  );
}

const styles = StyleSheet.create({
  row: { flexDirection: "row", paddingVertical: 3 },
  key: { width: 96 },
  val: { flex: 1 },
  mono: { fontFamily: "monospace", fontSize: 12, lineHeight: 17 },
});
