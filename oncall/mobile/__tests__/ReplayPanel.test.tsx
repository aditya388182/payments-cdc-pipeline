import { act, fireEvent, render, screen, waitFor } from "@testing-library/react-native";
import React from "react";
import { ApiError } from "@/api/client";
import type { DlqRecord, ReplayResult, ReplayStatus } from "@/api/types";
import { ReplayPanel } from "@/components/ReplayPanel";

let mockUuid = 0;
jest.mock("expo-crypto", () => ({ randomUUID: () => `key-${String(++mockUuid).padStart(8, "0")}` }));

const record: DlqRecord = {
  id: "108", transaction_id: "18b9fe64-64d7-4eb4-834a-50f353f5f28f", merchant_id: "MERCH_DEMO_A", amount_minor: 4200, currency: "USD", status: "PENDING",
  event_type: "AUTHORIZATION", op: "c", lsn: 30847456, rejection_reason: "UNKNOWN_MERCHANT", rejected_at: "2026-11-16T06:35:00Z", applied: false,
};

const result = (status: ReplayStatus, detail = "detail text"): ReplayResult => ({ dlq_id: "108", status, detail, lake_lsn: 1 });

describe("ReplayPanel: role-based hiding", () => {
  it("shows no Replay button to a viewer, only an explanation", async () => {
    const api = { replay: jest.fn() };
    await render(<ReplayPanel record={record} role="viewer" api={api} />);
    expect(screen.queryByTestId("replay-button")).toBeNull();
    expect(screen.queryByText("Replay record")).toBeNull();
    expect(screen.getByTestId("replay-admin-only")).toBeTruthy();
    expect(api.replay).not.toHaveBeenCalled();
  });

  it("shows nothing actionable when the role is unknown (not signed in)", async () => {
    await render(<ReplayPanel record={record} role={null} api={{ replay: jest.fn() }} />);
    expect(screen.queryByTestId("replay-button")).toBeNull();
  });

  it("shows the Replay button to an admin", async () => {
    await render(<ReplayPanel record={record} role="admin" api={{ replay: jest.fn() }} />);
    expect(screen.getByTestId("replay-button")).toBeTruthy();
    expect(screen.getByLabelText("Replay record")).toBeTruthy();
  });
});

describe("ReplayPanel: confirmation sheet and result states", () => {
  it("does nothing until the admin confirms, and Cancel closes the sheet without calling the API", async () => {
    const api = { replay: jest.fn() };
    await render(<ReplayPanel record={record} role="admin" api={api} />);
    expect(screen.queryByTestId("confirm-sheet")).toBeNull();
    await fireEvent.press(screen.getByTestId("replay-button"));
    expect(screen.getByTestId("confirm-sheet")).toBeTruthy();
    expect(screen.getByText(/UNKNOWN_MERCHANT/)).toBeTruthy();
    expect(api.replay).not.toHaveBeenCalled();
    await fireEvent.press(screen.getByTestId("confirm-cancel"));
    expect(screen.queryByTestId("confirm-sheet")).toBeNull();
    expect(api.replay).not.toHaveBeenCalled();
  });

  it.each<[ReplayStatus, string, string]>([
    ["APPLIED", "Applied", "now in the lake"],
    ["ALREADY_APPLIED", "Already applied", "nothing was written twice"],
    ["STILL_INVALID", "Still invalid", "validator still rejects"],
    ["PENDING", "Published, not visible yet", "Retry in a couple of minutes"],
    ["NOT_FOUND", "Not found", "no longer exists"],
  ])("renders the %s result", async (status, title, help) => {
    const api = { replay: jest.fn(async () => result(status, `server says ${status}`)) };
    const seen: ReplayResult[] = [];
    await render(<ReplayPanel record={record} role="admin" api={api} onResult={(r) => seen.push(r)} />);
    await fireEvent.press(screen.getByTestId("replay-button"));
    await fireEvent.press(screen.getByTestId("confirm-ok"));
    await waitFor(() => expect(screen.getByTestId("replay-result")).toBeTruthy());
    expect(screen.getByTestId("replay-status").props.accessibilityLabel).toBe(status);
    expect(screen.getByText(title)).toBeTruthy();
    expect(screen.getByText(new RegExp(help))).toBeTruthy();
    expect(screen.getByTestId("replay-detail").props.children).toBe(`server says ${status}`);
    expect(screen.queryByTestId("confirm-sheet")).toBeNull();
    expect(seen).toHaveLength(1);
  });

  it("sends one Idempotency-Key per attempt and a fresh one for a new attempt after a finished result", async () => {
    const api = { replay: jest.fn(async (_id: string, _key?: string) => result("APPLIED")) };
    await render(<ReplayPanel record={record} role="admin" api={api} />);
    for (let i = 0; i < 2; i++) {
      await fireEvent.press(screen.getByTestId("replay-button"));
      await fireEvent.press(screen.getByTestId("confirm-ok"));
      await waitFor(() => expect(api.replay).toHaveBeenCalledTimes(i + 1));
    }
    const keys = api.replay.mock.calls.map((c) => c[1]);
    expect(keys[0]).not.toBe(keys[1]);
    expect(api.replay.mock.calls[0]?.[0]).toBe("108");
  });

  it("reuses the SAME key when retrying after a network error or a PENDING result (so it cannot apply twice)", async () => {
    const api = {
      replay: jest
        .fn<Promise<ReplayResult>, [string, string]>()
        .mockRejectedValueOnce(new ApiError(503, "replay_unavailable", "the replay service is unavailable; retry shortly"))
        .mockResolvedValueOnce(result("PENDING"))
        .mockResolvedValueOnce(result("APPLIED")),
    };
    await render(<ReplayPanel record={record} role="admin" api={api as unknown as { replay: (id: string, key?: string) => Promise<ReplayResult> }} />);
    const attempt = async (n: number): Promise<void> => {
      await fireEvent.press(screen.getByTestId("replay-button"));
      await fireEvent.press(screen.getByTestId("confirm-ok"));
      await waitFor(() => expect(api.replay).toHaveBeenCalledTimes(n));
    };
    await attempt(1);
    await waitFor(() => expect(screen.getByTestId("replay-error")).toBeTruthy());
    expect(screen.getByText(/replay service is unavailable/)).toBeTruthy();
    await attempt(2);
    await waitFor(() => expect(screen.getByTestId("replay-status").props.accessibilityLabel).toBe("PENDING"));
    await attempt(3);
    await waitFor(() => expect(screen.getByTestId("replay-status").props.accessibilityLabel).toBe("APPLIED"));
    const keys = api.replay.mock.calls.map((c) => c[1]);
    expect(new Set(keys).size).toBe(1);
  });

  it("disables the confirm button while the request is running (no double submit)", async () => {
    let release: (r: ReplayResult) => void = () => undefined;
    const api = { replay: jest.fn(() => new Promise<ReplayResult>((res) => { release = res; })) };
    await render(<ReplayPanel record={record} role="admin" api={api} />);
    await fireEvent.press(screen.getByTestId("replay-button"));
    await fireEvent.press(screen.getByTestId("confirm-ok"));
    await fireEvent.press(screen.getByTestId("confirm-ok"));
    expect(api.replay).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("confirm-ok").props.accessibilityState.disabled).toBe(true);
    await act(async () => release(result("APPLIED")));
    await waitFor(() => expect(screen.getByTestId("replay-result")).toBeTruthy());
  });

  it("labels the button when the record is already in the lake", async () => {
    await render(<ReplayPanel record={{ ...record, applied: true }} role="admin" api={{ replay: jest.fn() }} />);
    expect(screen.getByLabelText("Replay (already applied)")).toBeTruthy();
  });
});
