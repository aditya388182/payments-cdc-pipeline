import Constants from "expo-constants";
import { Platform } from "react-native";

export type PushResult = { ok: true; token: string } | { ok: false; reason: string };

/**
 * Registers this phone for push. expo-notifications and expo-device are native modules: they are required lazily and
 * only off the web, so the web build and Jest never load them. On a real phone this needs a development build
 * (remote push is not available in Expo Go on Android since SDK 53) and an EAS projectId; see docs/oncall/BLOCKERS.md.
 */
export async function registerForPush(): Promise<PushResult> {
  if (Platform.OS === "web") return { ok: false, reason: "Push alerts need the app on a phone (a development build)." };
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const Device = require("expo-device") as typeof import("expo-device");
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const Notifications = require("expo-notifications") as typeof import("expo-notifications");
  if (!Device.isDevice) return { ok: false, reason: "Push tokens are only issued to physical devices." };

  if (Platform.OS === "android") {
    await Notifications.setNotificationChannelAsync("alerts", {
      name: "Pipeline alerts",
      importance: Notifications.AndroidImportance.HIGH,
    });
  }
  const existing = await Notifications.getPermissionsAsync();
  let status = existing.status;
  if (status !== "granted") status = (await Notifications.requestPermissionsAsync()).status;
  if (status !== "granted") return { ok: false, reason: "Notification permission was not granted." };

  const projectId = (Constants.expoConfig?.extra as { eas?: { projectId?: string } } | undefined)?.eas?.projectId ?? Constants.easConfig?.projectId;
  if (!projectId) return { ok: false, reason: "No EAS projectId is configured for this build." };
  const token = (await Notifications.getExpoPushTokenAsync({ projectId })).data;
  return { ok: true, token };
}

/** Show alerts that arrive while the app is open. Native only. */
export function installForegroundHandler(): void {
  if (Platform.OS === "web") return;
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const Notifications = require("expo-notifications") as typeof import("expo-notifications");
  Notifications.setNotificationHandler({
    handleNotification: async () => ({ shouldShowBanner: true, shouldShowList: true, shouldPlaySound: true, shouldSetBadge: false }),
  });
}
