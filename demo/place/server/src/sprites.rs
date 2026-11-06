//! The ambient agents' pictures: small pixel sprites in palette colors,
//! each with a few color variants. An agent paints one sprite whole, in
//! pushes of at most `tilesPerPush` tiles, so the canvas fills in with
//! recognizable shapes rather than noise and visitors' work stands out.
//!
//! A sprite is ASCII art: `.` is transparent (left as it is), and `1` to
//! `4` are roles a variant maps to colors (lowercase `#rrggbb`, which must
//! be in the deployment's palette; variants with a missing color are left
//! out).

use crate::rules::{PaletteEntry, Tile};

/// One sprite: its art and its color variants (role 1 first).
#[derive(Debug, Clone, Copy)]
pub struct Sprite {
    /// For logs and tests.
    pub name: &'static str,
    /// Rows of the same length.
    pub art: &'static [&'static str],
    /// Colors per role, one array per variant.
    pub variants: &'static [&'static [&'static str]],
}

impl Sprite {
    /// Width and height in tiles.
    pub fn size(&self) -> (u32, u32) {
        (self.art[0].len() as u32, self.art.len() as u32)
    }

    /// The tiles of variant `v` with the top-left corner at `(x, y)`, in
    /// row order; `None` when a color of the variant is not in `palette`.
    pub fn tiles(&self, v: usize, x: u32, y: u32, palette: &[PaletteEntry]) -> Option<Vec<Tile>> {
        let colors: Vec<u8> = self.variants[v]
            .iter()
            .map(|c| palette.iter().position(|p| p.color == *c).map(|i| i as u8))
            .collect::<Option<_>>()?;
        let mut out = Vec::new();
        for (dy, row) in self.art.iter().enumerate() {
            for (dx, ch) in row.bytes().enumerate() {
                if let Some(role) = ch.checked_sub(b'1').map(usize::from)
                    && let Some(&color) = colors.get(role)
                {
                    out.push(Tile {
                        x: x + dx as u32,
                        y: y + dy as u32,
                        color,
                    });
                }
            }
        }
        Some(out)
    }
}

const RED: &str = "#d6262b";
const PINK: &str = "#ff99aa";
const MAGENTA: &str = "#b44ac0";
const PURPLE: &str = "#6d3a8c";
const YELLOW: &str = "#ffd635";
const ORANGE: &str = "#ff7a1a";
const WHITE: &str = "#ffffff";
const LIGHT: &str = "#d4d7d9";
const GREY: &str = "#898d90";
const BLACK: &str = "#1b1f22";
const BROWN: &str = "#9c6926";
const LIME: &str = "#7eed56";
const GREEN: &str = "#00a368";
const CYAN: &str = "#51e9f4";
const BLUE: &str = "#3690ea";
const NAVY: &str = "#2f3ab4";

/// The built-in set.
#[rustfmt::skip]
pub const SPRITES: &[Sprite] = &[
    Sprite {
        name: "heart",
        art: &[
            ".11.11.",
            "1111111",
            "1111111",
            "1111111",
            ".11111.",
            "..111..",
            "...1...",
        ],
        variants: &[&[RED], &[PINK], &[MAGENTA], &[PURPLE]],
    },
    Sprite {
        name: "star",
        art: &[
            "....1....",
            "....1....",
            "...111...",
            "111111111",
            ".1111111.",
            "..11111..",
            "..11.11..",
            ".11...11.",
            ".1.....1.",
        ],
        variants: &[&[YELLOW], &[ORANGE], &[WHITE], &[CYAN]],
    },
    Sprite {
        name: "smiley",
        art: &[
            "..2222..",
            ".211112.",
            "21211212",
            "21111112",
            "21211212",
            "21122112",
            ".211112.",
            "..2222..",
        ],
        variants: &[&[YELLOW, BLACK], &[LIME, BLACK], &[CYAN, NAVY], &[PINK, PURPLE]],
    },
    Sprite {
        name: "tree",
        art: &[
            "...1...",
            "..111..",
            ".11111.",
            "..111..",
            ".11111.",
            "1111111",
            "..111..",
            ".11111.",
            "1111111",
            "...2...",
            "...2...",
        ],
        variants: &[&[GREEN, BROWN], &[LIME, BROWN], &[WHITE, BROWN]],
    },
    Sprite {
        name: "rocket",
        art: &[
            "...1...",
            "..111..",
            "..111..",
            ".11111.",
            ".11211.",
            ".12221.",
            ".11211.",
            ".11111.",
            "4111114",
            "44.1.44",
            "..3.3..",
            "...3...",
        ],
        variants: &[
            &[WHITE, BLUE, ORANGE, RED],
            &[LIGHT, NAVY, YELLOW, BLUE],
            &[WHITE, CYAN, YELLOW, MAGENTA],
        ],
    },
    Sprite {
        name: "house",
        art: &[
            "....1....",
            "...111...",
            "..11111..",
            ".1111111.",
            "111111111",
            ".2222222.",
            ".2332332.",
            ".2222222.",
            ".2233222.",
            ".2233222.",
        ],
        variants: &[&[RED, WHITE, BROWN], &[BROWN, YELLOW, NAVY], &[NAVY, LIGHT, RED]],
    },
    Sprite {
        name: "cat",
        art: &[
            "1.......1",
            "11.....11",
            "111111111",
            "112111211",
            "111111111",
            ".1113111.",
            ".1111111.",
            ".11.1.11.",
            "..1...1..",
        ],
        variants: &[&[BLACK, YELLOW, PINK], &[ORANGE, GREEN, PINK], &[GREY, LIME, PINK]],
    },
    Sprite {
        name: "git-branch",
        art: &[
            ".1...1.",
            "111.111",
            ".1...1.",
            ".1...1.",
            ".1..1..",
            ".1.1...",
            ".11....",
            ".1.....",
            "111....",
            ".1.....",
        ],
        variants: &[&[ORANGE], &[GREEN], &[BLUE]],
    },
    Sprite {
        name: "reason-hexagon",
        art: &[
            "...111...",
            "..12221..",
            ".1222221.",
            "122222221",
            "122222221",
            "122222221",
            ".1222221.",
            "..12221..",
            "...111...",
        ],
        variants: &[&[NAVY, BLUE], &[BLACK, WHITE], &[PURPLE, MAGENTA]],
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::default_palette;

    #[test]
    fn every_sprite_is_rectangular_fits_a_few_pushes_and_uses_palette_colors() {
        let palette = default_palette();
        for s in SPRITES {
            let (w, _) = s.size();
            assert!(s.art.iter().all(|r| r.len() as u32 == w), "{}", s.name);
            assert!((7..=16).contains(&w), "{}", s.name);
            for v in 0..s.variants.len() {
                let tiles = s
                    .tiles(v, 0, 0, &palette)
                    .unwrap_or_else(|| panic!("{} {v}", s.name));
                assert!(!tiles.is_empty() && tiles.len() <= 3 * 64, "{}", s.name);
                let roles = s
                    .art
                    .iter()
                    .flat_map(|r| r.bytes())
                    .filter(|c| *c != b'.')
                    .max()
                    .unwrap();
                assert_eq!(
                    usize::from(roles - b'0'),
                    s.variants[v].len(),
                    "{} {v}",
                    s.name
                );
            }
        }
    }
}
