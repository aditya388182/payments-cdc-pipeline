import { Stack } from "expo-router";
import { StatusBar } from "expo-status-bar";
import React, { useEffect } from "react";
import { AuthProvider } from "@/auth/AuthContext";
import { installForegroundHandler } from "@/push/register";

export default function RootLayout(): React.JSX.Element {
  useEffect(() => installForegroundHandler(), []);
  return (
    <AuthProvider>
      <StatusBar style="auto" />
      <Stack>
        <Stack.Screen name="index" options={{ headerShown: false }} />
        <Stack.Screen name="login" options={{ headerShown: false }} />
        <Stack.Screen name="auth/callback" options={{ headerShown: false }} />
        <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
        <Stack.Screen name="dlq/[id]" options={{ title: "DLQ record", headerBackTitle: "DLQ" }} />
      </Stack>
    </AuthProvider>
  );
}
