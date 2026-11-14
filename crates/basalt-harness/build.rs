//! Embeds the commit this build came from as `BASALT_COMMIT`, because a failing seed reproduces
//! only on the same commit. A build whose sources differ from that commit gets a `-dirty` suffix,
//! and a build outside a git checkout gets `unknown`.

/// The paths, relative to the repository root, that a build compiles. Only changes here make a
/// build dirty, and only changes here rerun this script.
const SOURCES: [&str; 4] = ["crates", "Cargo.toml", "Cargo.lock", "rust-toolchain.toml"];

fn main() {
    let commit = match git(&["rev-parse", "--show-toplevel"]) {
        Some(root) => commit(&root),
        None => "unknown".to_owned(),
    };
    println!("cargo:rustc-env=BASALT_COMMIT={commit}");
}

/// Returns HEAD's hash, with `-dirty` if the sources differ from it, and tells Cargo when to
/// rerun this script.
fn commit(root: &str) -> String {
    let sources = SOURCES.map(|path| format!("{root}/{path}"));

    // Rerun after a checkout or commit, which move HEAD or the branch it points to, and after a
    // source edit, which can change the dirty flag.
    let mut watched = sources.to_vec();
    for name in ["HEAD", "packed-refs"] {
        watched.extend(git(&["rev-parse", "--git-path", name]));
    }
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watched.extend(git(&["rev-parse", "--git-path", &branch]));
    }
    // Cargo reruns on every build when a watched path doesn't exist, such as a packed branch ref.
    for path in watched
        .iter()
        .filter(|path| std::path::Path::new(path).exists())
    {
        println!("cargo:rerun-if-changed={path}");
    }

    let Some(hash) = git(&["rev-parse", "HEAD"]) else {
        return "unknown".to_owned();
    };
    // `--no-optional-locks` keeps `status` from refreshing the index. A refresh would take
    // index.lock, failing a concurrent `git commit`.
    let mut status = vec![
        "--no-optional-locks",
        "status",
        "--porcelain",
        "--untracked-files=no",
    ];
    status.push("--");
    status.extend(sources.iter().map(String::as_str));
    match git(&status) {
        Some(changes) if !changes.is_empty() => format!("{hash}-dirty"),
        _ => hash,
    }
}

/// Runs git in the package directory and returns its trimmed output, or `None` if it fails.
#[allow(
    clippy::disallowed_types,
    reason = "build scripts run git on the build host"
)]
fn git(args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}
