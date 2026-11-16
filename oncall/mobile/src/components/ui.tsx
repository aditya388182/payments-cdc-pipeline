import React from "react";
import { ActivityIndicator, Pressable, StyleSheet, Text, View, type StyleProp, type TextStyle, type ViewStyle } from "react-native";
import { space, usePalette } from "@/theme/theme";

export function Card({ children, style }: { children: React.ReactNode; style?: StyleProp<ViewStyle> }): React.JSX.Element {
  const p = usePalette();
  return <View style={[styles.card, { backgroundColor: p.card, borderColor: p.border }, style]}>{children}</View>;
}

export function Heading({ children, style }: { children: React.ReactNode; style?: StyleProp<TextStyle> }): React.JSX.Element {
  const p = usePalette();
  return (
    <Text accessibilityRole="header" style={[styles.h, { color: p.text }, style]}>
      {children}
    </Text>
  );
}

export function Body({ children, muted, style, testID }: { children: React.ReactNode; muted?: boolean; style?: StyleProp<TextStyle>; testID?: string }): React.JSX.Element {
  const p = usePalette();
  return <Text testID={testID} style={[styles.body, { color: muted ? p.muted : p.text }, style]}>{children}</Text>;
}

export type Tone = "good" | "warn" | "bad" | "info" | "muted";

export function Pill({ label, tone, testID }: { label: string; tone: Tone; testID?: string }): React.JSX.Element {
  const p = usePalette();
  const color = tone === "muted" ? p.muted : p[tone];
  return (
    <View testID={testID} accessible accessibilityLabel={label} style={[styles.pill, { borderColor: color }]}>
      <Text style={[styles.pillText, { color }]}>{label}</Text>
    </View>
  );
}

export function Button({
  label, onPress, disabled, tone = "primary", testID, accessibilityHint,
}: {
  label: string; onPress: () => void; disabled?: boolean; tone?: "primary" | "danger" | "plain"; testID?: string; accessibilityHint?: string;
}): React.JSX.Element {
  const p = usePalette();
  const bg = tone === "primary" ? p.primary : tone === "danger" ? p.bad : "transparent";
  const fg = tone === "plain" ? p.primary : p.onPrimary;
  return (
    <Pressable
      testID={testID}
      accessibilityRole="button"
      accessibilityLabel={label}
      accessibilityHint={accessibilityHint}
      accessibilityState={{ disabled: !!disabled }}
      disabled={disabled}
      onPress={onPress}
      style={({ pressed }) => [styles.btn, { backgroundColor: bg, borderColor: tone === "plain" ? p.border : bg, opacity: disabled ? 0.5 : pressed ? 0.8 : 1 }]}
    >
      <Text style={[styles.btnText, { color: fg }]}>{label}</Text>
    </Pressable>
  );
}

export function Loading({ label = "Loading" }: { label?: string }): React.JSX.Element {
  const p = usePalette();
  return (
    <View accessibilityRole="progressbar" accessibilityLabel={label} style={styles.center} testID="loading">
      <ActivityIndicator color={p.primary} />
      <Body muted style={{ marginTop: space.sm }}>{label}…</Body>
    </View>
  );
}

export function EmptyState({ title, hint }: { title: string; hint?: string }): React.JSX.Element {
  return (
    <View style={styles.center} testID="empty-state" accessible accessibilityLabel={title}>
      <Heading>{title}</Heading>
      {hint ? <Body muted>{hint}</Body> : null}
    </View>
  );
}

export function ErrorState({ message, onRetry }: { message: string; onRetry?: () => void }): React.JSX.Element {
  const p = usePalette();
  return (
    <View style={styles.center} testID="error-state" accessibilityRole="alert">
      <Text style={[styles.h, { color: p.bad }]}>Something went wrong</Text>
      <Body muted style={{ textAlign: "center", marginVertical: space.sm }}>{message}</Body>
      {onRetry ? <Button label="Try again" onPress={onRetry} tone="plain" /> : null}
    </View>
  );
}

export function Screen({ children }: { children: React.ReactNode }): React.JSX.Element {
  const p = usePalette();
  return <View style={[styles.screen, { backgroundColor: p.bg }]}>{children}</View>;
}

const styles = StyleSheet.create({
  screen: { flex: 1 },
  card: { borderWidth: 1, borderRadius: 12, padding: space.lg, marginBottom: space.md },
  h: { fontSize: 18, fontWeight: "700" },
  body: { fontSize: 15, lineHeight: 21 },
  pill: { borderWidth: 1.5, borderRadius: 999, paddingHorizontal: 10, paddingVertical: 3, alignSelf: "flex-start" },
  pillText: { fontSize: 12, fontWeight: "700", letterSpacing: 0.3 },
  btn: { minHeight: 44, borderRadius: 10, borderWidth: 1, paddingHorizontal: space.lg, alignItems: "center", justifyContent: "center" },
  btnText: { fontSize: 16, fontWeight: "600" },
  center: { flex: 1, alignItems: "center", justifyContent: "center", padding: space.xl },
});
