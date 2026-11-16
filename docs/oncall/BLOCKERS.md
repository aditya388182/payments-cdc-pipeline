# Blockers: Phase 6 is yours (accounts, money, a phone, secrets)

Everything the build could do without those is done and tested against a mock GitHub OAuth server and a fake push sender.
Until you have done the steps below, the honest wording is: **"tested with a fake push sender and a mock OAuth server"**.
Nothing here needs code changes except where a step says so.

Ports you will use through your SSH tunnel to this box (the console's services listen on 127.0.0.1 only; see DONE.md for the original compose services):

| Port | What | Needed for |
|---|---|---|
| 8088 | `oncall-api` (REST + WebSocket + OAuth) | the app, always |
| 19006 | Expo **web** build (only while `make oncall-e2e` or `node scripts/serve-web.mjs dist` runs) | trying the web build in your browser |
| 8099 | mock GitHub (demo only) | not needed once you use real GitHub |
| 3000 / 9090 | Grafana / Prometheus | optional |

```
ssh -N -L 127.0.0.1:8088:127.0.0.1:8088 -L 127.0.0.1:19006:127.0.0.1:19006 ubuntu@<this-box>
```

---

## 1. GitHub OAuth app (the real sign-in)

Why it is yours: it needs your GitHub account and creates a client secret.

1. GitHub -> **Settings** -> **Developer settings** -> **OAuth Apps** -> **New OAuth App**.
2. Fill in:
   - Application name: `On-Call Console (dev)`
   - Homepage URL: the public base URL of the API as your browser sees it (see "Which URL" below), for example `http://localhost:8088`
   - Authorization callback URL: **`<that same base URL>/v1/auth/github/callback`**
3. **Register application**, copy the **Client ID**, click **Generate a new client secret** and copy it once.
4. Put the values OUTSIDE the repo (the repo's rules forbid committing them):
   ```
   mkdir -p ~/.config/oncall && chmod 700 ~/.config/oncall
   $EDITOR ~/.config/oncall/github.env     # then: chmod 600 ~/.config/oncall/github.env
   ```
   with exactly these lines (replace the angle-bracket parts):
   ```
   ONCALL_GITHUB_BASE=https://github.com
   ONCALL_GITHUB_API_BASE=https://api.github.com
   ONCALL_GITHUB_CLIENT_ID=<client id>
   ONCALL_GITHUB_CLIENT_SECRET=<client secret>
   ONCALL_PUBLIC_URL=<base URL, same as in step 2>
   ONCALL_ALLOWLIST=<your-github-login>
   ONCALL_ADMINS=<your-github-login>
   ```
   (`ONCALL_ALLOWLIST` is the comma-separated list of GitHub logins that may sign in; `ONCALL_ADMINS` is the subset that gets the admin role. Logins are case-insensitive.)
5. Run the API with the real GitHub instead of the mock: `oncall/tools/run_api_real.sh` (it loads `.env` and then `~/.config/oncall/github.env` and starts `oncall-api`; it never prints either file). Stop the mock-backed API first with `oncall/tools/stack.sh down` or `kill` its pid from `.run/api.pid`.
6. Check it from your laptop browser first: open `<base URL>/v1/auth/github/start?code_challenge=<43+ chars>&redirect_uri=http%3A%2F%2F127.0.0.1%3A19006%2Fauth%2Fcallback` and confirm you land on GitHub's authorize page. (The app builds this URL itself; this is just a smoke test.)

**Which URL** (`ONCALL_PUBLIC_URL`): it must be a URL that the browser doing the sign-in can open.
- Signing in from the **laptop browser** (the Expo web build): `http://localhost:8088` works through the SSH tunnel above.
- Signing in from the **phone**: the phone cannot reach your laptop's `localhost`. Two options:
  - an HTTPS tunnel to the laptop, no account needed: `cloudflared tunnel --url http://localhost:8088` prints an `https://<random>.trycloudflare.com` URL. Use it as `ONCALL_PUBLIC_URL`, as the GitHub callback base, and as `EXPO_PUBLIC_API_URL` for the app. The hostname changes each run, so update the GitHub app's callback URL each time.
  - or bind your SSH tunnel on the LAN (`-L 0.0.0.0:8088:127.0.0.1:8088`) and use `http://<laptop-LAN-IP>:8088`. Check GitHub's form accepts an `http://` non-localhost callback; if it refuses, use the HTTPS option.

What to expect: GitHub asks you to authorize "On-Call Console (dev)", then the app opens with your login and role shown in Settings.

## 2. The app on your phone

Expo Go is enough for **sign-in and the live/alerts/DLQ/audit screens** on both platforms. **Push needs a development build** (see section 3): remote push from `expo-notifications` is not available in Expo Go on Android since SDK 53.

Quick try with Expo Go (no accounts):
1. On your laptop (needs Node 20+): `git clone` this branch, then
   ```
   cd oncall/mobile && npm ci
   EXPO_PUBLIC_API_URL=<API base URL the phone can reach> npx expo start
   ```
2. Install **Expo Go** from the App Store / Play Store, scan the QR code the command prints (laptop and phone on the same Wi-Fi; if it does not connect use `npx expo start --tunnel`).
3. Tap **Sign in with GitHub**. The redirect back to the app is `exp://...` in Expo Go; the API accepts the `exp://` and `oncall://` schemes.

## 3. Real push notifications (development build + Expo/Firebase/Apple accounts)

Why it is yours: Expo account, Firebase project (Android), Apple Developer Program (iOS, USD 99/year), a physical phone.

Requirements, from the current Expo docs (`docs.expo.dev/push-notifications/push-notifications-setup`):
- a **development build** (not Expo Go on Android),
- **Android:** Firebase Cloud Messaging V1 credentials uploaded to Expo,
- **iOS:** a paid Apple Developer account, the iPhone registered for internal distribution, and an APNs key (the EAS CLI offers to generate it),
- the EAS **projectId** in the app config.

Steps (run on your laptop in `oncall/mobile`):
1. Create an Expo account at expo.dev, then `npx eas-cli login`.
2. `npx eas-cli init` : creates the project and writes `extra.eas.projectId` into `app.json`. Commit that one change yourself.
3. **Android:** create a Firebase project (console.firebase.google.com), add an Android app with package name **`dev.oncall.console`**, download `google-services.json` into `oncall/mobile/` (it is git-ignored), then Project settings -> Service accounts -> **Generate new private key**. Upload that key to Expo: `npx eas-cli credentials` -> Android -> your build profile -> *Google Service Account Key for FCM V1* (docs: `docs.expo.dev/push-notifications/fcm-credentials`). Add `"googleServicesFile": "./google-services.json"` under `expo.android` in `app.json`.
4. **iOS:** `npx eas-cli device:create` (registers your iPhone), then answer **yes** to "Setup Push Notifications" and "Generate a new Apple Push Notifications service key" on the first iOS build.
5. Build and install the development client: `npx eas-cli build --profile development --platform android` (or `ios`). The repo's `eas.json` already defines the `development` profile. Open the install link on the phone.
6. Start Metro for the dev client: `EXPO_PUBLIC_API_URL=<API base URL the phone can reach> npx expo start --dev-client`, open the dev build, connect.
7. Sign in with GitHub (section 1). In **Settings**, switch **Push alerts** on and allow notifications. The app registers its Expo push token with `POST /v1/devices`.
8. Switch the API to the real sender: add `ONCALL_PUSH_SENDER=expo` to `~/.config/oncall/github.env` (and `ONCALL_EXPO_ACCESS_TOKEN=<token>` only if you enabled "enhanced push security" for the project in expo.dev) and restart `oncall/tools/run_api_real.sh`.
9. Trigger an alert: start the generator from the app's API (`POST /v1/generator`, rate 150) or run `python scripts/data_generator.py --rate 150 --duration 120` on the box. After the lag crosses the threshold for 30 s you should get a notification **"ALERT: consumer_lag_high"**. Acknowledge it in the app and you get **"Acknowledged"**; when the lag drains, **"Resolved"**.
10. If nothing arrives: `curl -s https://exp.host/--/api/v2/push/getReceipts` style checks are not needed first; instead send a test from `https://expo.dev/notifications` using the token shown by `docker exec postgres psql -U oncall -d oncall -c "select token, platform from devices"` (run on the box with the password from `.env`; do not paste the token anywhere public). A push that works there but not from the API points at `ONCALL_PUSH_SENDER` or the access token; one that fails there points at the FCM/APNs credentials.

## 4. After you have done 1-3

- Record what you verified (which phone, OS, date) in `docs/oncall/PROGRESS.md`; then and only then change the README/resume wording from "tested with a fake push sender and a mock OAuth server" to include "verified on a physical device".
- Remove the test OAuth app and the Firebase key if you no longer need them.
