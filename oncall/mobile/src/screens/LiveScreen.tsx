import React from "react";
import { ScrollView, StyleSheet, View } from "react-native";
import type { ParityState } from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { LineChart } from "@/components/LineChart";
import { fmtInt, fmtRate, fmtSeconds } from "@/components/formatting";
import { Body, Card, Heading, Loading, Pill, Screen, type Tone } from "@/components/ui";
import { useLive } from "@/live/useLive";
import { space, usePalette } from "@/theme/theme";

const PARITY: Record<ParityState, { label: string; tone: Tone }> = {
  in_sync: { label: "In sync", tone: "good" },
  catching_up: { label: "Catching up", tone: "warn" },
  mismatch: { label: "Mismatch", tone: "bad" },
  unknown: { label: "Unknown", tone: "muted" },
};

function Tile({ label, value, sub, testID }: { label: string; value: string; sub?: string; testID?: string }): React.JSX.Element {
  const p = usePalette();
  return (
    <View testID={testID} accessible accessibilityLabel={`${label}: ${value}${sub ? `, ${sub}` : ""}`} style={[styles.tile, { backgroundColor: p.card, borderColor: p.border }]}>
      <Body muted style={styles.tileLabel}>{label}</Body>
      <Heading style={styles.tileValue}>{value}</Heading>
      {sub ? <Body muted style={styles.tileSub}>{sub}</Body> : null}
    </View>
  );
}

export function LiveScreen(): React.JSX.Element {
  const { tokens, signOut } = useAuth();
  const live = useLive(() => tokens.getAccessToken(), () => void tokens.refresh().catch(() => void signOut()));
  const p = usePalette();
  const s = live.latest;
  const connected = live.status === "open";

  return (
    <Screen>
      <View
        testID="connection-banner"
        accessibilityLiveRegion="polite"
        accessibilityLabel={connected ? "Connected, live" : live.status === "connecting" ? "Connecting" : `Reconnecting, attempt ${String(live.attempt)}`}
        style={[styles.banner, { backgroundColor: connected ? p.card : p.warn, borderColor: p.border }]}
      >
        <View style={[styles.dot, { backgroundColor: connected ? p.good : p.bad }]} />
        <Body style={{ color: connected ? p.text : p.bg, fontWeight: "600" }}>
          {connected ? "Live · updates every 2 s" : live.status === "connecting" ? "Connecting…" : `Reconnecting… (attempt ${String(live.attempt)})`}
        </Body>
      </View>
      {!s ? (
        <Loading label="Waiting for the first snapshot" />
      ) : (
        <ScrollView contentContainerStyle={{ padding: space.lg }} testID="live-scroll">
          <View style={styles.row}>
            <Card style={styles.flex}>
              <Body muted>Parity (source vs lake)</Body>
              <View style={{ marginVertical: space.sm }}>
                <Pill testID="parity-badge" label={PARITY[s.parity.state].label} tone={PARITY[s.parity.state].tone} />
              </View>
              <Body muted style={styles.tileSub}>
                {fmtInt(s.source_count)} source · {fmtInt(s.lake_count)} lake · diff {fmtInt(s.parity.diff)}
              </Body>
            </Card>
            <Card style={styles.flex}>
              <Body muted>Connector</Body>
              <View style={{ marginVertical: space.sm }}>
                <Pill testID="connector-status" label={s.connector.state} tone={s.connector.state === "RUNNING" ? "good" : "bad"} />
              </View>
              <Body muted style={styles.tileSub}>Debezium · {(s.connector.tasks ?? []).length} task(s)</Body>
            </Card>
          </View>
          <View style={styles.row}>
            <Tile testID="tile-lag" label="Consumer lag" value={fmtInt(s.lag_total)} sub={s.caught_up ? "caught up" : "records behind"} />
            <Tile testID="tile-freshness" label="Freshness p95" value={fmtSeconds(s.freshness_p95_seconds)} sub={`p50 ${fmtSeconds(s.freshness_p50_seconds)}`} />
          </View>
          <View style={styles.row}>
            <Tile testID="tile-in" label="Events in /s" value={fmtRate(s.events_in_per_sec)} />
            <Tile testID="tile-out" label="Merged /s" value={fmtRate(s.events_out_per_sec)} />
            <Tile testID="tile-dlq" label="DLQ open" value={fmtInt(s.dlq.open)} sub={`+${String(s.dlq.growth_per_min)}/min`} />
          </View>
          <Card>
            <Heading style={{ marginBottom: space.sm }}>Throughput</Heading>
            <LineChart
              testID="chart-throughput"
              unit="/s"
              series={[
                { label: "events in", values: live.history.map((h) => h.events_in_per_sec), color: p.chart1 },
                { label: "merged out", values: live.history.map((h) => h.events_out_per_sec), color: p.chart2 },
              ]}
            />
          </Card>
          <Card>
            <Heading style={{ marginBottom: space.sm }}>Consumer lag</Heading>
            <LineChart testID="chart-lag" series={[{ label: "records behind", values: live.history.map((h) => h.lag_total), color: p.bad }]} />
          </Card>
          <Body muted style={{ textAlign: "center" }} testID="snapshot-seq">snapshot #{s.seq}</Body>
        </ScrollView>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  banner: { flexDirection: "row", alignItems: "center", gap: 8, paddingHorizontal: space.lg, paddingVertical: space.sm, borderBottomWidth: 1 },
  dot: { width: 10, height: 10, borderRadius: 5 },
  row: { flexDirection: "row", gap: space.md, marginBottom: space.md },
  flex: { flex: 1, marginBottom: 0 },
  tile: { flex: 1, borderWidth: 1, borderRadius: 12, padding: space.md },
  tileLabel: { fontSize: 12 },
  tileValue: { fontSize: 22 },
  tileSub: { fontSize: 12 },
});
