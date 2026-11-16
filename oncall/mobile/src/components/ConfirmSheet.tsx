import React from "react";
import { Modal, Pressable, StyleSheet, View } from "react-native";
import { space, usePalette } from "@/theme/theme";
import { Body, Button, Heading } from "./ui";

interface Props {
  visible: boolean;
  title: string;
  message: string;
  confirmLabel: string;
  busy?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}

/** A bottom confirmation sheet for destructive/admin actions. */
export function ConfirmSheet({ visible, title, message, confirmLabel, busy, onConfirm, onCancel }: Props): React.JSX.Element {
  const p = usePalette();
  return (
    <Modal visible={visible} transparent animationType="slide" onRequestClose={onCancel} accessibilityViewIsModal>
      <Pressable style={styles.backdrop} onPress={onCancel} accessibilityLabel="Dismiss" accessibilityRole="button" />
      <View testID="confirm-sheet" accessibilityRole="alert" style={[styles.sheet, { backgroundColor: p.card, borderColor: p.border }]}>
        <Heading>{title}</Heading>
        <Body muted style={{ marginVertical: space.md }}>{message}</Body>
        <View style={styles.row}>
          <View style={styles.flex}><Button label="Cancel" tone="plain" onPress={onCancel} disabled={busy} testID="confirm-cancel" /></View>
          <View style={styles.flex}><Button label={busy ? "Working…" : confirmLabel} onPress={onConfirm} disabled={busy} testID="confirm-ok" /></View>
        </View>
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  backdrop: { flex: 1, backgroundColor: "rgba(0,0,0,0.45)" },
  sheet: { position: "absolute", left: 0, right: 0, bottom: 0, padding: space.xl, borderTopLeftRadius: 18, borderTopRightRadius: 18, borderWidth: 1 },
  row: { flexDirection: "row", gap: space.md },
  flex: { flex: 1 },
});
