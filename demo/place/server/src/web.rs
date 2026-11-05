//! The page's static files, served by `aspen-place` itself when
//! `PLACE_WEB_DIR` names a built front end (`npm run build` in `demo/place`,
//! which writes `dist/`). One process then answers the page, the API and
//! `canvas.git` on one host, which is how the AWS deployment runs it
//! (`deploy/terraform/aws/public-demo`).
//!
//! Every file is read into memory once, at startup, and served from there:
//! the build is a few hundred kilobytes, and no request ever touches the
//! file system, so no request path can name a file outside the directory.
//! Each file gets one exact route; `/` is `index.html`. A file at or under
//! a path the API and Git router keeps ([`aspen_place::reserved_paths`])
//! refuses the start: its route would collide with the API's, which axum
//! answers with a panic naming no file, or shadow Git.

use std::collections::BTreeMap;
use std::path::Path;

use axum::Router;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;

/// The most a web directory may hold. A built page is far smaller; anything
/// this large is the wrong directory.
const MAX_TOTAL_BYTES: u64 = 64 << 20;
/// The most files a web directory may hold.
const MAX_FILES: usize = 2_000;

/// One file to serve.
#[derive(Debug, Clone)]
struct File {
    body: Bytes,
    content_type: &'static str,
    cache_control: &'static str,
}

/// The files of a built page, keyed by their URL path (`/assets/x.js`).
#[derive(Debug, Clone, Default)]
pub(crate) struct WebFiles {
    files: BTreeMap<String, File>,
}

impl WebFiles {
    /// Reads every file under `dir`. Refuses symbolic links, names that are
    /// not plain URL path segments, a file whose URL path is one of
    /// `reserved` or lies under one, more than [`MAX_FILES`] files or
    /// [`MAX_TOTAL_BYTES`] in all, and a directory without `index.html`.
    pub(crate) fn load(dir: &Path, reserved: &[String]) -> Result<Self, String> {
        let mut files = BTreeMap::new();
        let mut total = 0u64;
        let mut pending = vec![(dir.to_path_buf(), String::new())];
        while let Some((path, prefix)) = pending.pop() {
            let entries = std::fs::read_dir(&path)
                .map_err(|e| format!("PLACE_WEB_DIR: reading {}: {e}", path.display()))?;
            for entry in entries {
                let entry = entry.map_err(|e| format!("PLACE_WEB_DIR: {e}"))?;
                let name = entry.file_name();
                let name = name
                    .to_str()
                    .filter(|n| is_plain_segment(n))
                    .ok_or_else(|| {
                        format!(
                            "PLACE_WEB_DIR: {} is not a plain file name (letters, digits, '.', '-', '_')",
                            entry.path().display()
                        )
                    })?
                    .to_owned();
                let kind = entry
                    .file_type()
                    .map_err(|e| format!("PLACE_WEB_DIR: {e}"))?;
                let url = format!("{prefix}/{name}");
                if kind.is_dir() {
                    pending.push((entry.path(), url));
                } else if kind.is_file() {
                    if let Some(prefix) = reserved.iter().find(|p| is_at_or_under(&url, p)) {
                        return Err(format!(
                            "PLACE_WEB_DIR: {} would be served at {url}, but the server keeps {prefix} \
                             for its API, its own routes or the canvas repository; rename or remove the file",
                            entry.path().display()
                        ));
                    }
                    let body = std::fs::read(entry.path())
                        .map_err(|e| format!("PLACE_WEB_DIR: reading {url}: {e}"))?;
                    total += body.len() as u64;
                    if files.len() >= MAX_FILES || total > MAX_TOTAL_BYTES {
                        return Err(format!(
                            "PLACE_WEB_DIR: more than {MAX_FILES} files or {MAX_TOTAL_BYTES} bytes; is it the built page?"
                        ));
                    }
                    files.insert(
                        url.clone(),
                        File {
                            body: Bytes::from(body),
                            content_type: content_type(&name),
                            cache_control: cache_control(&url),
                        },
                    );
                } else {
                    return Err(format!(
                        "PLACE_WEB_DIR: {url} is a symbolic link or special file; only plain files are served"
                    ));
                }
            }
        }
        if !files.contains_key("/index.html") {
            return Err(format!(
                "PLACE_WEB_DIR: {} has no index.html; point it at the output of `npm run build`",
                dir.display()
            ));
        }
        Ok(Self { files })
    }

    /// One `GET` (and so `HEAD`) route per file, plus `/` for `index.html`.
    /// Merge it with the API and Git router: it has no fallback of its own.
    pub(crate) fn router(self) -> Router {
        let mut router = Router::new();
        let index = self.files.get("/index.html").cloned();
        for (url, file) in self.files {
            router = router.route(&url, get(move || std::future::ready(respond(&file))));
        }
        if let Some(file) = index {
            router = router.route("/", get(move || std::future::ready(respond(&file))));
        }
        router
    }
}

fn respond(file: &File) -> Response {
    (
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static(file.content_type),
            ),
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static(file.cache_control),
            ),
            (
                header::X_CONTENT_TYPE_OPTIONS,
                HeaderValue::from_static("nosniff"),
            ),
        ],
        file.body.clone(),
    )
        .into_response()
}

/// A file or directory name that is also a literal axum route segment and
/// needs no escaping: what Vite writes (`index-3f2a1c.js`).
fn is_plain_segment(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
}

/// Whether `url` is the path `prefix` or a path below it (`/api/v1/x` is
/// under `/api/v1`; `/api/v10` is not).
fn is_at_or_under(url: &str, prefix: &str) -> bool {
    url.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

fn content_type(name: &str) -> &'static str {
    match name.rsplit_once('.').map(|(_, ext)| ext) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json" | "map") => "application/json",
        Some("webmanifest") => "application/manifest+json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("webp") => "image/webp",
        Some("woff2") => "font/woff2",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Vite puts a content hash in every name under `assets/`, so those never
/// change; everything else (`index.html` first) is checked on every load.
fn cache_control(url: &str) -> &'static str {
    if url.starts_with("/assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    /// What the binary passes: the default canvas repository's.
    fn reserved() -> Vec<String> {
        aspen_place::reserved_paths(&aspen_place::PlaceConfig::new(
            "https://place.test/canvas.git",
        ))
    }

    fn page() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<!doctype html>place").unwrap();
        std::fs::create_dir(dir.path().join("assets")).unwrap();
        std::fs::write(dir.path().join("assets/index-abc123.js"), "console.log(1)").unwrap();
        std::fs::write(dir.path().join("favicon.svg"), "<svg/>").unwrap();
        dir
    }

    async fn get(
        router: &Router,
        path: &str,
    ) -> (StatusCode, Option<String>, Option<String>, Bytes) {
        let response = router
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let header = |name| {
            response
                .headers()
                .get(name)
                .map(|v: &HeaderValue| v.to_str().unwrap().to_owned())
        };
        let (ct, cc) = (header(header::CONTENT_TYPE), header(header::CACHE_CONTROL));
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, ct, cc, body)
    }

    #[tokio::test]
    async fn the_page_and_its_assets_are_served_with_their_types_and_caching() {
        let dir = page();
        let router = WebFiles::load(dir.path(), &reserved()).unwrap().router();

        let (status, ct, cc, body) = get(&router, "/").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ct.as_deref(), Some("text/html; charset=utf-8"));
        assert_eq!(cc.as_deref(), Some("no-cache"));
        assert_eq!(&body[..], b"<!doctype html>place");

        let (status, ct, cc, body) = get(&router, "/assets/index-abc123.js").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ct.as_deref(), Some("text/javascript; charset=utf-8"));
        assert_eq!(cc.as_deref(), Some("public, max-age=31536000, immutable"));
        assert_eq!(&body[..], b"console.log(1)");

        let (_, ct, cc, _) = get(&router, "/favicon.svg").await;
        assert_eq!(ct.as_deref(), Some("image/svg+xml"));
        assert_eq!(cc.as_deref(), Some("no-cache"));
    }

    #[tokio::test]
    async fn other_paths_are_left_to_the_router_it_is_merged_with() {
        let dir = page();
        let api = Router::new().route("/api/v1/config", axum::routing::get(|| async { "api" }));
        let router = WebFiles::load(dir.path(), &reserved())
            .unwrap()
            .router()
            .merge(api);
        assert_eq!(
            get(&router, "/api/v1/config").await.3,
            Bytes::from_static(b"api")
        );
        for path in [
            "/assets/missing.js",
            "/../Cargo.toml",
            "/assets/../index.html",
            "/index.html/x",
        ] {
            assert_eq!(get(&router, path).await.0, StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[test]
    fn a_directory_without_index_html_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("app.js"), "x").unwrap();
        let e = WebFiles::load(dir.path(), &reserved()).unwrap_err();
        assert!(e.contains("no index.html"), "{e}");
    }

    #[test]
    fn names_that_are_not_plain_route_segments_are_refused() {
        for name in ["{id}.js", "*.js", "a b.js", ".env", "%2e.js"] {
            let dir = page();
            std::fs::write(dir.path().join(name), "x").unwrap();
            let e = WebFiles::load(dir.path(), &reserved()).unwrap_err();
            assert!(e.contains("not a plain file name"), "{name}: {e}");
        }
    }

    /// A file at or under a path the API and Git router keeps is refused,
    /// naming the file, instead of panicking in `Router::merge` (API
    /// routes) or shadowing a route (Aspen's own, Git's fallback) (#146).
    #[test]
    fn files_at_or_under_reserved_paths_are_refused_by_name() {
        for (url, prefix) in [
            ("api/v1/config", "/api/v1"),
            ("api/v1/admin/kill-switch", "/api/v1"),
            ("api/v1/no-such-route", "/api/v1"),
            ("api/v1", "/api/v1"),
            ("-/metrics", "/-"),
            ("-/blobs/x", "/-"),
            ("-/express/x", "/-"),
            ("-", "/-"),
            ("canvas.git/info/refs", "/canvas.git"),
            ("canvas.git/-/refs/heads/x/git-receive-pack", "/canvas.git"),
            ("canvas/git-upload-pack", "/canvas"),
            ("canvas.git", "/canvas.git"),
            ("canvas", "/canvas"),
        ] {
            let dir = page();
            let file = dir.path().join(url);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(&file, "x").unwrap();
            let e = WebFiles::load(dir.path(), &reserved()).unwrap_err();
            assert!(
                e.contains(&file.display().to_string())
                    && e.contains(&format!("served at /{url},"))
                    && e.contains(&format!("keeps {prefix} ")),
                "{url}: {e}"
            );
        }
    }

    #[tokio::test]
    async fn names_that_only_start_like_a_reserved_path_are_served() {
        let dir = page();
        let urls = [
            "api.js",
            "api/v10.js",
            "api/v2/x.json",
            "apis/v1/x.json",
            "-x.js",
            "canvas.png",
            "canvas.gitx",
            "canvases/x.png",
        ];
        for url in urls {
            let file = dir.path().join(url);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(&file, url).unwrap();
        }
        let router = WebFiles::load(dir.path(), &reserved()).unwrap().router();
        for url in urls {
            let (status, _, _, body) = get(&router, &format!("/{url}")).await;
            assert_eq!(status, StatusCode::OK, "{url}");
            assert_eq!(&body[..], url.as_bytes());
        }
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_links_are_refused() {
        let dir = page();
        std::os::unix::fs::symlink("/etc/hostname", dir.path().join("host.txt")).unwrap();
        let e = WebFiles::load(dir.path(), &reserved()).unwrap_err();
        assert!(e.contains("symbolic link"), "{e}");
    }
}
