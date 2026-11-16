import { Redirect } from "expo-router";
import React from "react";
import { useAuth } from "@/auth/AuthContext";
import { Loading, Screen } from "@/components/ui";

export default function Index(): React.JSX.Element {
  const { status } = useAuth();
  if (status === "loading") return <Screen><Loading /></Screen>;
  return <Redirect href={status === "signedIn" ? "/live" : "/login"} />;
}
