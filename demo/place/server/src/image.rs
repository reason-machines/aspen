//! The canvas as an image: `GET /api/v1/canvas.svg`, for READMEs and
//! links. One SVG of `main`'s tiles, crisp at any size, rendered once per
//! `main` commit and kept in memory ([`SvgCache`]), so serving it never
//! reads Git.
//!
//! Each palette color is one `<path>` of its horizontal runs, so even a
//! busy 64 x 64 canvas stays small (tens of kilobytes); unpainted tiles
//! show the page's faint checker.

use std::fmt::Write as _;
use std::sync::Mutex;

use aspen_core::id::ObjectId;
use bytes::Bytes;

use crate::rules::PaletteEntry;
use crate::state::UNSET;

/// The rendered width or height of the longer side, in CSS pixels.
const DISPLAY_PX: u32 = 720;

/// `pixels` (row-major palette indexes, [`UNSET`] for none) as an SVG.
pub fn canvas_svg(pixels: &[u8], width: u32, height: u32, palette: &[PaletteEntry]) -> String {
    let scale = (DISPLAY_PX / width.max(height).max(1)).max(1);
    let mut out = String::with_capacity(16 * 1024);
    let _ = write!(
        out,
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {width} {height}" shape-rendering="crispEdges"><title>place canvas</title><defs><pattern id="u" width="2" height="2" patternUnits="userSpaceOnUse"><rect width="2" height="2" fill="#fafafa"/><rect x="1" width="1" height="1" fill="#f1f1f1"/><rect y="1" width="1" height="1" fill="#f1f1f1"/></pattern></defs><rect width="{width}" height="{height}" fill="url(#u)"/>"##,
        w = width * scale,
        h = height * scale,
    );
    let w = width as usize;
    let mut paths = vec![String::new(); palette.len()];
    for (y, row) in pixels.chunks(w.max(1)).enumerate() {
        let mut x = 0;
        while x < row.len() {
            let c = row[x];
            let start = x;
            while x < row.len() && row[x] == c {
                x += 1;
            }
            if c != UNSET && usize::from(c) < paths.len() {
                let _ = write!(
                    paths[usize::from(c)],
                    "M{start} {y}h{}v1h-{}z",
                    x - start,
                    x - start
                );
            }
        }
    }
    for (entry, d) in palette.iter().zip(&paths) {
        if !d.is_empty() {
            let _ = write!(out, r#"<path fill="{}" d="{d}"/>"#, entry.color);
        }
    }
    out.push_str("</svg>");
    out
}

/// The last rendering, by `main` commit.
#[derive(Debug, Default)]
pub struct SvgCache(Mutex<Option<(ObjectId, Bytes)>>);

impl SvgCache {
    /// The SVG of `commit`, rendered with `render` only when the cached one
    /// is of another commit.
    pub fn get(&self, commit: ObjectId, render: impl FnOnce() -> String) -> Bytes {
        let mut cached = self.0.lock().expect("not poisoned");
        if let Some((c, svg)) = cached.as_ref()
            && *c == commit
        {
            return svg.clone();
        }
        let svg = Bytes::from(render());
        *cached = Some((commit, svg.clone()));
        svg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::default_palette;

    #[test]
    fn runs_of_one_color_are_one_path_and_unpainted_tiles_are_left_out() {
        let palette = default_palette();
        let mut pixels = vec![UNSET; 4 * 2];
        pixels[0] = 1;
        pixels[1] = 1;
        pixels[3] = 2;
        pixels[5] = 1;
        let svg = canvas_svg(&pixels, 4, 2, &palette);
        assert!(svg.starts_with("<svg "), "{svg}");
        assert!(svg.contains(r#"viewBox="0 0 4 2""#), "{svg}");
        assert!(svg.contains(r#"width="720" height="360""#), "{svg}");
        assert!(svg.contains(r#"shape-rendering="crispEdges""#));
        assert!(
            svg.contains(&format!(
                r#"<path fill="{}" d="M0 0h2v1h-2zM1 1h1v1h-1z"/>"#,
                palette[1].color
            )),
            "{svg}"
        );
        assert!(
            svg.contains(&format!(
                r#"<path fill="{}" d="M3 0h1v1h-1z"/>"#,
                palette[2].color
            )),
            "{svg}"
        );
        assert_eq!(svg.matches("<path").count(), 2);
    }

    #[test]
    fn the_cache_renders_once_per_commit() {
        let cache = SvgCache::default();
        let a = ObjectId::from_bytes([1; 20]);
        let renders = std::cell::Cell::new(0);
        let render = || {
            renders.set(renders.get() + 1);
            String::from("<svg/>")
        };
        cache.get(a, render);
        cache.get(a, render);
        assert_eq!(renders.get(), 1);
        cache.get(ObjectId::from_bytes([2; 20]), render);
        assert_eq!(renders.get(), 2);
    }
}
