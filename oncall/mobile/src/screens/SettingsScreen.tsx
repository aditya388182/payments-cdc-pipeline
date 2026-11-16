import React, { useEffect, useState } from "react";
import { Platform, ScrollView, Switch, View } from "react-native";
import { useAuth } from "@/auth/AuthContext";
import { defaultStore } from "@/auth/storage";
import { Body, Button, Card, Heading, Pill, Screen } from "@/components/ui";
import { apiBaseUrl } from "@/config";
import { registerForPush } from "@/push/register";
import { space } from "@/theme/theme";

const PUSH_KEY = "oncall.push.token";

export function SettingsScreen(): React.JSX.Element {
  const { user, role, api, signOut } = useAuth();
  const [store] = useState(defaultStore);
  const [token, setToken] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    void store.get(PUSH_KEY).then(setToken);
  }, [store]);

  const toggle = async (on: boolean): Promise<void> => {
    setBusy(true);
    setMsg(null);
    try {
      if (on) {
        const r = await registerForPush();
        if (!r.ok) {
          setMsg(r.reason);
          return;
        }
        await api.registerDevice(r.token, Platform.OS);
        await store.set(PUSH_KEY, r.token);
        setToken(r.token);
        setMsg("Push alerts are on for this device.");
      } else if (token) {
        await api.unregisterDevice(token);
        await store.remove(PUSH_KEY);
        setToken(null);
        setMsg("Push alerts are off for this device.");
      }
    } catch (e) {
      setMsg(e instanceof Error ? e.message : "Could not change push settings.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <Screen>
      <ScrollView contentContainerStyle={{ padding: space.lg }} testID="settings-scroll">
        <Card>
          <Heading>Signed in</Heading>
          <Body testID="settings-user" style={{ marginVertical: space.sm }}>{user?.login ?? "—"}</Body>
          <Pill testID="settings-role" label={(role ?? "unknown").toUpperCase()} tone={role === "admin" ? "info" : "muted"} />
          <Body muted style={{ marginTop: space.sm }}>
            {role === "admin" ? "You can replay DLQ records and see the audit log." : "Read-only: you can watch the pipeline and acknowledge alerts."}
          </Body>
        </Card>
        <Card>
          <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>
            <View style={{ flex: 1, paddingRight: space.md }}>
              <Heading>Push alerts</Heading>
              <Body muted>Get a notification when lag, freshness, the connector, the DLQ or parity cross a threshold.</Body>
            </View>
            <Switch
              testID="push-switch"
              accessibilityLabel="Push alerts"
              value={token !== null}
              disabled={busy}
              onValueChange={(v) => void toggle(v)}
            />
          </View>
          {msg ? <Body testID="push-message" style={{ marginTop: space.md }}>{msg}</Body> : null}
        </Card>
        <Card>
          <Heading>Server</Heading>
          <Body muted testID="settings-api">{apiBaseUrl()}</Body>
        </Card>
        <Button label="Sign out" tone="danger" onPress={() => void signOut()} testID="sign-out" />
      </ScrollView>
    </Screen>
  );
}
