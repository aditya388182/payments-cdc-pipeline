import { useColorScheme } from "react-native";

export interface Palette {
  bg: string;
  card: string;
  text: string;
  muted: string;
  border: string;
  primary: string;
  onPrimary: string;
  good: string;
  warn: string;
  bad: string;
  info: string;
  chart1: string;
  chart2: string;
}

const light: Palette = {
  bg: "#f4f6f8", card: "#ffffff", text: "#13202c", muted: "#5b6b7a", border: "#dde3e9", primary: "#1a56db", onPrimary: "#ffffff",
  good: "#127a3f", warn: "#a15c00", bad: "#c0262d", info: "#1a56db", chart1: "#1a56db", chart2: "#b45309",
};
const dark: Palette = {
  bg: "#0e141b", card: "#17202a", text: "#e8eef4", muted: "#9aabbb", border: "#2a3846", primary: "#6ea0ff", onPrimary: "#0e141b",
  good: "#4cc38a", warn: "#f0b24a", bad: "#ff7b80", info: "#6ea0ff", chart1: "#6ea0ff", chart2: "#f0b24a",
};

export function usePalette(): Palette {
  return useColorScheme() === "dark" ? dark : light;
}

export const space = { xs: 4, sm: 8, md: 12, lg: 16, xl: 24 } as const;
