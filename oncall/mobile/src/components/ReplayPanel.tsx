import React, { useRef, useState } from "react";
import { View } from "react-native";
import { ApiError } from "@/api/client";
import type { DlqRecord, ReplayResult, ReplayStatus, Role } from "@/api/types";
import { newIdempotencyKey } from "@/auth/pkce";
import { space } from "@/theme/theme";
import { ConfirmSheet } from "./ConfirmSheet";
import { Body, Button, Card, Heading, Pill, type Tone } from "./ui";

export interface ReplayApi {
  replay: (id: string, idempotencyKey?: string) => Promise<ReplayResult>;
}

const COPY: Record<ReplayStatus, { tone: Tone; title: string; help: string }> = {
  APPLIED: { tone: "good", title: "Applied", help: "The record passed validation, was re-published through the normal pipeline and is now in the lake." },
  ALREADY_APPLIED: { tone: "info", title: "Already applied", help: "The lake already had this record, so nothing was written twice." },
  STILL_INVALID: { tone: "bad", title: "Still invalid", help: "The validator still rejects this record. Fix the cause (for example the reference data), then replay again." },
  PENDING: { tone: "warn", title: "Published, not visible yet", help: "The record was re-published but the lake does not have it yet. Retry in a couple of minutes." },
  NOT_FOUND: { tone: "bad", title: "Not found", help: "This DLQ record no longer exists." },
};

interface Props {
  record: DlqRecord;
  role: Role | null;
  api: ReplayApi;
  onResult?: (r: ReplayResult) => void;
}

/** Replay is an admin action: viewers see an explanation instead of the button. Every attempt reuses one Idempotency-Key. */
export function ReplayPanel({ record, role, api, onResult }: Props): React.JSX.Element {
  const [confirming, setConfirming] = useState(false);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<ReplayResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const key = useRef<string | null>(null);

  if (role !== "admin") {
    return (
      <Card>
        <Body muted testID="replay-admin-only">Only admins can replay records.</Body>
      </Card>
    );
  }

  const run = async (): Promise<void> => {
    setBusy(true);
    setError(null);
    key.current ??= newIdempotencyKey();
    try {
      const r = await api.replay(record.id, key.current);
      setResult(r);
      if (r.status !== "PENDING") key.current = null; // a finished attempt starts fresh next time; PENDING retries with the same key
      onResult?.(r);
      setConfirming(false);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not reach the server.");
      setConfirming(false);
    } finally {
      setBusy(false);
    }
  };

  const copy = result ? COPY[result.status] : null;
  return (
    <View testID="replay-panel">
      <Card>
        <Heading>Replay</Heading>
        <Body muted style={{ marginVertical: space.sm }}>
          Re-runs the validator on this record and, if it passes, publishes it back through the normal pipeline. Replaying twice never writes two rows.
        </Body>
        <Button
          label={record.applied ? "Replay (already applied)" : "Replay record"}
          onPress={() => setConfirming(true)}
          testID="replay-button"
          accessibilityHint="Opens a confirmation sheet"
        />
      </Card>
      {result && copy ? (
        <Card>
          <View testID="replay-result" accessibilityLiveRegion="polite">
            <Pill label={result.status} tone={copy.tone} testID="replay-status" />
            <Heading style={{ marginTop: space.sm }}>{copy.title}</Heading>
            <Body muted>{copy.help}</Body>
            <Body style={{ marginTop: space.sm }} testID="replay-detail">{result.detail}</Body>
          </View>
        </Card>
      ) : null}
      {error ? (
        <Card>
          <View testID="replay-error" accessibilityRole="alert">
            <Body>{error}</Body>
            <Body muted>Retrying uses the same Idempotency-Key, so it cannot apply twice.</Body>
          </View>
        </Card>
      ) : null}
      <ConfirmSheet
        visible={confirming}
        title="Replay this record?"
        message={`Transaction ${record.transaction_id.slice(0, 8)}… was rejected as ${record.rejection_reason}. This is audited under your name.`}
        confirmLabel="Replay"
        busy={busy}
        onConfirm={() => void run()}
        onCancel={() => setConfirming(false)}
      />
    </View>
  );
}
