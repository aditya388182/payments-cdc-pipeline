import { render, screen } from "@testing-library/react-native";
import React from "react";
import type { Role } from "@/api/types";
import { AuthContextForTests, type AuthValue } from "@/auth/AuthContext";

const mockScreens: { name: string; options: Record<string, unknown> }[] = [];
jest.mock("expo-router", () => {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { View } = require("react-native") as typeof import("react-native");
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const ReactMod = require("react") as typeof import("react");
  const Tabs = ({ children }: { children: React.ReactNode }) => ReactMod.createElement(View, { testID: "tabs" }, children);
  Tabs.Screen = ({ name, options }: { name: string; options: Record<string, unknown> }) => {
    mockScreens.push({ name, options });
    return null;
  };
  return { Tabs, Redirect: ({ href }: { href: string }) => ReactMod.createElement(View, { testID: `redirect:${href}` }) };
});

import TabsLayout from "../app/(tabs)/_layout";

const auth = (status: AuthValue["status"], role: Role | null): AuthValue =>
  ({ status, role, user: role ? { login: "x", role } : null, api: {}, tokens: {}, error: null, signIn: jest.fn(), signOut: jest.fn(), completeSignIn: jest.fn() }) as unknown as AuthValue;

const renderLayout = async (a: AuthValue): Promise<void> => {
  mockScreens.length = 0;
  await render(<AuthContextForTests.Provider value={a}><TabsLayout /></AuthContextForTests.Provider>);
};

describe("role-based tab visibility", () => {
  it("hides the Audit tab from a viewer", async () => {
    await renderLayout(auth("signedIn", "viewer"));
    const audit = mockScreens.find((s) => s.name === "audit");
    expect(audit?.options.href).toBeNull();
    expect(mockScreens.map((s) => s.name)).toEqual(["live", "alerts", "dlq", "audit", "settings"]);
    for (const s of mockScreens.filter((x) => x.name !== "audit")) expect(s.options.href).toBeUndefined();
  });

  it("shows the Audit tab to an admin", async () => {
    await renderLayout(auth("signedIn", "admin"));
    expect(mockScreens.find((s) => s.name === "audit")?.options.href).toBeUndefined();
  });

  it("every tab has an accessibility label", async () => {
    await renderLayout(auth("signedIn", "admin"));
    for (const s of mockScreens) expect(s.options.tabBarAccessibilityLabel).toEqual(expect.any(String));
  });

  it("sends a signed-out user to the login screen instead of rendering tabs", async () => {
    await renderLayout(auth("signedOut", null));
    expect(screen.getByTestId("redirect:/login")).toBeTruthy();
    expect(mockScreens).toHaveLength(0);
  });
});
