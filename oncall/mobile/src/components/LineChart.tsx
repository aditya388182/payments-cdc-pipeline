import React from "react";
import { StyleSheet, Text, View } from "react-native";
import Svg, { Line, Polyline, Text as SvgText } from "react-native-svg";
import { usePalette } from "@/theme/theme";

export interface Series {
  label: string;
  values: number[];
  color: string;
}

interface Props {
  series: Series[];
  height?: number;
  width?: number;
  unit?: string;
  testID?: string;
}

/** A small dependency-free line chart (react-native-svg), readable by screen readers through its summary label. */
export function LineChart({ series, height = 120, width = 320, unit = "", testID }: Props): React.JSX.Element {
  const p = usePalette();
  const all = series.flatMap((s) => s.values);
  const max = Math.max(1, ...all);
  const pad = 22;
  const innerW = width - pad - 4;
  const innerH = height - 14;
  const summary = series
    .map((s) => `${s.label} now ${String(Math.round((s.values[s.values.length - 1] ?? 0) * 10) / 10)}${unit}`)
    .join(", ");
  const pts = (vals: number[]): string =>
    vals
      .map((v, i) => {
        const x = pad + (vals.length <= 1 ? innerW : (i / (vals.length - 1)) * innerW);
        const y = 4 + innerH - (v / max) * innerH;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");
  return (
    <View testID={testID} accessible accessibilityRole="image" accessibilityLabel={`Chart. ${summary}`}>
      <Svg width="100%" height={height} viewBox={`0 0 ${String(width)} ${String(height)}`}>
        <Line x1={pad} y1={4 + innerH} x2={width - 4} y2={4 + innerH} stroke={p.border} strokeWidth={1} />
        <Line x1={pad} y1={4} x2={pad} y2={4 + innerH} stroke={p.border} strokeWidth={1} />
        <SvgText x={2} y={12} fontSize={9} fill={p.muted}>{String(Math.round(max))}</SvgText>
        <SvgText x={2} y={4 + innerH} fontSize={9} fill={p.muted}>0</SvgText>
        {series.map((s) => (s.values.length > 0 ? <Polyline key={s.label} points={pts(s.values)} fill="none" stroke={s.color} strokeWidth={2} /> : null))}
      </Svg>
      <View style={styles.legend}>
        {series.map((s) => (
          <View key={s.label} style={styles.legendItem}>
            <View style={[styles.swatch, { backgroundColor: s.color }]} />
            <Text style={{ color: p.muted, fontSize: 12 }}>{s.label}</Text>
          </View>
        ))}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  legend: { flexDirection: "row", gap: 14, marginTop: 2 },
  legendItem: { flexDirection: "row", alignItems: "center", gap: 6 },
  swatch: { width: 10, height: 10, borderRadius: 2 },
});
