import { Redirect, useLocalSearchParams } from "expo-router";
import React, { useEffect, useRef, useState } from "react";
import { useAuth } from "@/auth/AuthContext";
import { Body, Button, ErrorState, Loading, Screen } from "@/components/ui";

/** Web redirect target: finishes the PKCE exchange. (On a phone the deep link is handled by openAuthSessionAsync.) */
export default function AuthCallback(): React.JSX.Element {
  const { code, error } = useLocalSearchParams<{ code?: string; error?: string }>();
  const { completeSignIn, status } = useAuth();
  const [failed, setFailed] = useState<string | null>(null);
  const started = useRef(false);
  const early = error
    ? error === "not_allowed" ? "This GitHub account is not allowed to use the console." : "Sign-in was not completed."
    : !code ? "The sign-in response did not include a code." : null;

  useEffect(() => {
    if (started.current || early) return;
    started.current = true;
    completeSignIn(String(code)).catch((e: unknown) => setFailed(e instanceof Error ? e.message : "Sign-in failed."));
  }, [code, early, completeSignIn]);

  if (status === "signedIn") return <Redirect href="/live" />;
  const problem = early ?? failed;
  if (problem)
    return (
      <Screen>
        <ErrorState message={problem} />
        <Body muted style={{ textAlign: "center" }}>You can close this and try again.</Body>
        <Button label="Back to sign in" onPress={() => globalThis.location.assign("/login")} testID="back-to-login" />
      </Screen>
    );
  return <Screen><Loading label="Signing you in" /></Screen>;
}
