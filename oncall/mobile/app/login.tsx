import { Redirect } from "expo-router";
import React from "react";
import { useAuth } from "@/auth/AuthContext";
import { LoginScreen } from "@/screens/LoginScreen";

export default function Login(): React.JSX.Element {
  const { status } = useAuth();
  if (status === "signedIn") return <Redirect href="/live" />;
  return <LoginScreen />;
}
