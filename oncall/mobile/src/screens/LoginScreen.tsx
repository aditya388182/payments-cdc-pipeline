import React, { useState } from "react";
import { StyleSheet, View } from "react-native";
import { useAuth } from "@/auth/AuthContext";
import { Body, Button, Heading, Screen } from "@/components/ui";
import { space } from "@/theme/theme";

export function LoginScreen(): React.JSX.Element {
  const { signIn, error } = useAuth();
  const [busy, setBusy] = useState(false);
  return (
    <Screen>
      <View style={styles.wrap}>
        <Heading style={styles.title}>On-Call Console</Heading>
        <Body muted style={styles.sub}>
          Watch the payments pipeline live, triage dead-lettered records and replay them safely.
        </Body>
        <Button
          label={busy ? "Opening GitHub…" : "Sign in with GitHub"}
          testID="sign-in"
          disabled={busy}
          accessibilityHint="Opens GitHub in a browser to sign in"
          onPress={() => {
            setBusy(true);
            void signIn().finally(() => setBusy(false));
          }}
        />
        {error ? (
          <View accessibilityRole="alert" accessibilityLiveRegion="polite">
            <Body testID="login-error" style={styles.err}>{error}</Body>
          </View>
        ) : null}
        <Body muted style={styles.fine}>Access is limited to allow-listed GitHub accounts.</Body>
      </View>
    </Screen>
  );
}

const styles = StyleSheet.create({
  wrap: { flex: 1, justifyContent: "center", padding: space.xl, gap: space.lg, maxWidth: 480, width: "100%", alignSelf: "center" },
  title: { fontSize: 30 },
  sub: { fontSize: 16 },
  err: { color: "#c0262d" },
  fine: { fontSize: 12, textAlign: "center" },
});
