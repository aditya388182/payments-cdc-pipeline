import { createHash, randomBytes } from "node:crypto";

jest.mock("expo-crypto", () => ({
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  getRandomBytes: (n: number) => new Uint8Array((require("node:crypto") as typeof import("node:crypto")).randomBytes(n)),
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  digestStringAsync: async (_alg: string, data: string) => (require("node:crypto") as typeof import("node:crypto")).createHash("sha256").update(data).digest("base64"),
  randomUUID: () => "00000000-0000-4000-8000-000000000000",
  CryptoDigestAlgorithm: { SHA256: "SHA-256" },
  CryptoEncoding: { BASE64: "base64" },
}));

import { base64ToBase64Url, base64UrlEncode, createPkcePair } from "@/auth/pkce";

describe("base64url", () => {
  it.each([
    [[], ""],
    [[0xff], "_w"],
    [[0xfb, 0xff], "-_8"],
    [[1, 2, 3], "AQID"],
    [[0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72], "Zm9vYmFy"],
  ])("encodes %j", (bytes, want) => {
    expect(base64UrlEncode(new Uint8Array(bytes))).toBe(want);
  });

  it("matches Node's own base64url for random input", () => {
    for (let i = 0; i < 20; i++) {
      const b = randomBytes(1 + i * 7);
      expect(base64UrlEncode(new Uint8Array(b))).toBe(b.toString("base64url"));
    }
  });

  it("converts standard base64 to base64url without padding", () => {
    expect(base64ToBase64Url("a+b/c==")).toBe("a-b_c");
  });
});

describe("createPkcePair (RFC 7636)", () => {
  it("makes a 64-character verifier and an S256 challenge that verifies", async () => {
    const { verifier, challenge } = await createPkcePair();
    expect(verifier).toMatch(/^[A-Za-z0-9_-]{64}$/);
    expect(challenge).toBe(createHash("sha256").update(verifier).digest("base64url"));
    expect(challenge).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });

  it("never repeats a verifier", async () => {
    const seen = new Set<string>();
    for (let i = 0; i < 25; i++) seen.add((await createPkcePair()).verifier);
    expect(seen.size).toBe(25);
  });
});
