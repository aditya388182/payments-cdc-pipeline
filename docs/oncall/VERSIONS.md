# Pinned versions (resolved 2026-11-16)

Resolved from the package registries on the build date; lockfiles (`go.sum`, `package-lock.json`, `req.lock.txt`, `oncall/replay/requirements.lock`) pin the transitive set.

| Area | Component | Version | Source of the number |
|---|---|---|---|
| Go | toolchain | 1.27.4 | `https://go.dev/dl/?mode=json`, sha256 verified |
| Go | go-chi/chi/v5 | v5.5.0 | `go list -m -f '{{.Version}}' ...@latest` |
| Go | jackc/pgx/v5 | v5.13.0 | same |
| Go | WebSocket: coder/websocket | v1.8.15 | same (gorilla/websocket is the alternative; coder/websocket is actively maintained, context-aware and has `CloseRead`) |
| Go | google.golang.org/grpc | v1.85.0 | same |
| Go | google.golang.org/protobuf | v1.36.12 | same |
| Go | twmb/franz-go + kadm | v1.22.3 / v1.20.0 | same |
| Go | prometheus/client_golang | v1.25.0 | same |
| Go | golang-jwt/jwt/v5 | v5.3.1 | same |
| Go | go.uber.org/goleak | v1.3.0 | same |
| Go | staticcheck (honnef.co/go/tools) | v0.8.2 | same |
| Proto | buf | v1.75.0 | GitHub release, checksum verified |
| Load | k6 | v2.4.0 | GitHub release, checksum verified |
| Python | CPython | 3.10.22 (venv via uv; box has 3.12) | README requires 3.10 |
| Python | grpcio / grpcio-tools / grpcio-health-checking | 1.85.1 | `uv pip compile` |
| Python | protobuf | 7.36.2 | same |
| Python | deltalake (delta-rs) | 1.6.6 | same |
| Python | pytest | 9.2.0 | same |
| Mobile | Expo SDK (`expo`) | 58.0.25 | `npm view expo version` |
| Mobile | react-native | 0.88.1 | `expo/bundledNativeModules.json` of expo@58.0.25 |
| Mobile | react | 19.3.0 (bundled) | same |
| Mobile | react-native-web | ~0.21.4 | same |
| Mobile | expo-notifications / secure-store / auth-session / web-browser / constants / linking | ~58.1.9 / ~58.0.3 / ~58.0.18 / ~58.0.8 / ~58.0.16 / ~58.0.17 | same |
| Mobile | expo-router | ~58.0.31 | same |
| Mobile | jest-expo | ~58.0.14 | same |
| Mobile | Jest | 30.5.2 | `npm view jest version` (final pin follows what jest-expo accepts) |
| Mobile | @testing-library/react-native | 14.1.0 | `npm view` |
| Mobile | Playwright (`@playwright/test`) | 1.65.1 | `npm view` |
| Infra | SeaweedFS | 4.53 | replaces MinIO, DECISIONS D-001 |

Node 24.22.1 and Java 21.0.13 were already on the box. Anything that needs a different version after `npm install` / `npx expo install` resolves it is corrected here and noted in DECISIONS.

## Expo push notifications: what a real device needs
Read from the current Expo docs (`docs.expo.dev/push-notifications/push-notifications-setup` and `.../overview`, fetched 2026-11-16):
- **Android: Expo Go is not enough.** The overview states remote push from `expo-notifications` is unavailable in Expo Go on Android from SDK 53, so a **development build** is required (`eas build` or a local build). It also needs **Firebase Cloud Messaging V1 credentials** uploaded to Expo (`docs.expo.dev/push-notifications/fcm-credentials`).
- **iOS:** a development build with push credentials; "A paid Apple Developer Account is required to generate credentials" (EAS asks to set up push and generate an APNs key on the first `eas build`), and the iPhone must be registered for internal distribution before the first build.
- `getExpoPushTokenAsync({ projectId })` needs the EAS `projectId` (`extra.eas.projectId`); it is set automatically when a development build is created.
- The token only comes from a **physical device** (or an Android emulator with Google Play services / a recent iOS simulator). Testing can use `https://expo.dev/notifications`.
- Sending uses the Expo push service (`https://exp.host/--/api/v2/push/send`); an access token is optional unless "enhanced security" is switched on for the project.
- Login/deep link: `expo-auth-session` and `expo-web-browser` work in Expo Go and in a development build; a custom `scheme` in `app.json` is what makes `oncall://` redirects work in a development build.
So: login and the live screens can be tried in Expo Go; push needs a development build on Android (and on iOS for a real token). This is written out as steps in BLOCKERS.md.
