import { useFocusEffect, useRouter } from "expo-router";
import React, { useCallback, useState } from "react";
import { FlatList, Pressable, View } from "react-native";
import type { DlqPage, DlqRecord } from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { fmtMoney, fmtTime, shortId } from "@/components/formatting";
import { Body, Button, Card, EmptyState, ErrorState, Heading, Loading, Pill, Screen } from "@/components/ui";
import { space } from "@/theme/theme";

const REASONS = ["", "UNKNOWN_MERCHANT", "INVALID_CURRENCY", "NEGATIVE_AMOUNT", "INVALID_UUID"] as const;

export function DlqListScreen(): React.JSX.Element {
  const { api } = useAuth();
  const router = useRouter();
  const [reason, setReason] = useState<string>("");
  const [records, setRecords] = useState<DlqRecord[]>([]);
  const [next, setNext] = useState("");
  const [openTotal, setOpenTotal] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [more, setMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(
    async (r: string): Promise<void> => {
      setLoading(true);
      setError(null);
      try {
        const page: DlqPage = await api.dlq({ reason: r || undefined });
        setRecords(page.records);
        setNext(page.next_cursor);
        setOpenTotal(page.open_total);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Could not load the DLQ.");
      } finally {
        setLoading(false);
      }
    },
    [api],
  );

  useFocusEffect(
    useCallback(() => {
      void load(reason);
    }, [load, reason]),
  );

  const loadMore = async (): Promise<void> => {
    if (!next) return;
    setMore(true);
    try {
      const page = await api.dlq({ reason: reason || undefined, cursor: next });
      setRecords((cur) => [...cur, ...page.records]);
      setNext(page.next_cursor);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load more.");
    } finally {
      setMore(false);
    }
  };

  return (
    <Screen>
      <View style={{ padding: space.lg, paddingBottom: space.sm }}>
        <Heading>Dead-letter queue{openTotal !== null ? ` · ${String(openTotal)} open` : ""}</Heading>
        <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8, marginTop: space.md }} testID="reason-filters">
          {REASONS.map((r) => (
            <Pressable
              key={r || "all"}
              testID={`filter-${r || "ALL"}`}
              accessibilityRole="button"
              accessibilityLabel={r ? `Filter by ${r.replace(/_/g, " ").toLowerCase()}` : "Show all reasons"}
              accessibilityState={{ selected: reason === r }}
              onPress={() => setReason(r)}
            >
              <Pill label={r ? r.replace(/_/g, " ") : "ALL"} tone={reason === r ? "info" : "muted"} />
            </Pressable>
          ))}
        </View>
      </View>
      {loading && records.length === 0 ? (
        <Loading label="Loading the DLQ" />
      ) : error && records.length === 0 ? (
        <ErrorState message={error} onRetry={() => void load(reason)} />
      ) : records.length === 0 ? (
        <EmptyState title="Nothing in the queue" hint="Records the validator rejects will show up here." />
      ) : (
        <FlatList
          testID="dlq-list"
          data={records}
          keyExtractor={(r) => r.id}
          contentContainerStyle={{ padding: space.lg, paddingTop: 0 }}
          refreshing={loading}
          onRefresh={() => void load(reason)}
          renderItem={({ item }) => (
            <Pressable
              testID={`dlq-row-${item.id}`}
              accessibilityRole="button"
              accessibilityLabel={`DLQ record ${shortId(item.transaction_id)}, ${item.rejection_reason.replace(/_/g, " ").toLowerCase()}, ${fmtMoney(item.amount_minor, item.currency)}`}
              onPress={() => router.push(`/dlq/${item.id}`)}
            >
              <Card>
                <Pill label={item.rejection_reason.replace(/_/g, " ")} tone="bad" />
                <Heading style={{ marginTop: space.sm }}>{shortId(item.transaction_id)}</Heading>
                <Body>{item.merchant_id} · {fmtMoney(item.amount_minor, item.currency)}</Body>
                <Body muted>rejected {fmtTime(item.rejected_at)}</Body>
              </Card>
            </Pressable>
          )}
          ListFooterComponent={
            next ? <Button label={more ? "Loading…" : "Load more"} tone="plain" onPress={() => void loadMore()} disabled={more} testID="dlq-more" /> : undefined
          }
        />
      )}
    </Screen>
  );
}
