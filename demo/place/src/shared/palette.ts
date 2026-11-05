/**
 * The default canvas palette. The server is the authority: clients read the
 * palette from `GET /config` and must not assume this list. It is fixed for
 * the lifetime of a canvas, because `CanvasSnapshot.pixels` encodes colors
 * as indexes into it.
 */
export interface PaletteEntry {
  /** Lowercase `#rrggbb`, exactly as it appears in a tile file. */
  color: string;
  /** Human-readable name, used for accessible labels. */
  name: string;
}

export const DEFAULT_PALETTE: readonly PaletteEntry[] = [
  { color: '#ffffff', name: 'White' },
  { color: '#d4d7d9', name: 'Light grey' },
  { color: '#898d90', name: 'Grey' },
  { color: '#1b1f22', name: 'Black' },
  { color: '#ff99aa', name: 'Pink' },
  { color: '#d6262b', name: 'Red' },
  { color: '#ff7a1a', name: 'Orange' },
  { color: '#9c6926', name: 'Brown' },
  { color: '#ffd635', name: 'Yellow' },
  { color: '#7eed56', name: 'Lime' },
  { color: '#00a368', name: 'Green' },
  { color: '#51e9f4', name: 'Cyan' },
  { color: '#3690ea', name: 'Blue' },
  { color: '#2f3ab4', name: 'Navy' },
  { color: '#b44ac0', name: 'Magenta' },
  { color: '#6d3a8c', name: 'Purple' },
];

/** At most 36 colors fit the one-character-per-pixel snapshot encoding. */
export const MAX_PALETTE_SIZE = 36;
