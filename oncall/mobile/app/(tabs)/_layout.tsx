import { Redirect, Tabs } from "expo-router";
import React from "react";
import { Text, type ColorValue } from "react-native";
import { useAuth } from "@/auth/AuthContext";
import { Loading, Screen } from "@/components/ui";
import { usePalette } from "@/theme/theme";

function Glyph({ glyph, color }: { glyph: string; color: ColorValue }): React.JSX.Element {
  return <Text style={{ color, fontSize: 18 }} accessible={false}>{glyph}</Text>;
}
const icon = (glyph: string) => function TabIcon({ color }: { color: ColorValue }): React.JSX.Element {
  return <Glyph glyph={glyph} color={color} />;
};

export default function TabsLayout(): React.JSX.Element {
  const { status, role } = useAuth();
  const p = usePalette();
  if (status === "loading") return <Screen><Loading /></Screen>;
  if (status === "signedOut") return <Redirect href="/login" />;
  return (
    <Tabs screenOptions={{ tabBarActiveTintColor: p.primary, tabBarStyle: { backgroundColor: p.card, borderTopColor: p.border }, headerStyle: { backgroundColor: p.card }, headerTintColor: p.text }}>
      <Tabs.Screen name="live" options={{ title: "Live", tabBarIcon: icon("◉"), tabBarButtonTestID: "tab-live", tabBarAccessibilityLabel: "Live" }} />
      <Tabs.Screen name="alerts" options={{ title: "Alerts", tabBarIcon: icon("▲"), tabBarButtonTestID: "tab-alerts", tabBarAccessibilityLabel: "Alerts" }} />
      <Tabs.Screen name="dlq" options={{ title: "DLQ", tabBarIcon: icon("☰"), tabBarButtonTestID: "tab-dlq", tabBarAccessibilityLabel: "Dead-letter queue" }} />
      <Tabs.Screen name="audit" options={{ title: "Audit", href: role === "admin" ? undefined : null, tabBarIcon: icon("✎"), tabBarButtonTestID: "tab-audit", tabBarAccessibilityLabel: "Audit log" }} />
      <Tabs.Screen name="settings" options={{ title: "Settings", tabBarIcon: icon("⚙"), tabBarButtonTestID: "tab-settings", tabBarAccessibilityLabel: "Settings" }} />
    </Tabs>
  );
}
