//! The playback preferences Soloist reads from its data directory.
//!
//! Soloist has no flag and no WebSocket command for the stream quality or for loudness
//! normalisation, and Spotify's documentation says it reads no configuration file. It does keep
//! per-account preferences in `settings/Users/<account>/prefs` under its data directory, and it
//! reads them as it starts. Other front ends for Soloist (Music Assistant, the Lyrion and Volumio
//! plugins, spotatui) set the keys below there.
//!
//! Spotify does not document any of this, so a Soloist build may rename or ignore a key, and
//! Soloist never reports what it streamed, so nothing here can tell. Soloist also rewrites the
//! file as it exits. The preferences are therefore only written while no Soloist runs, and a
//! failure to write them is for the caller to log and play on without.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::{Encoding, Offer, Quality};

/// The stream level, as Spotify numbers its tiers.
const ENUMERATION: &str = "audio.play_bitrate_enumeration";
/// The same level for a connection that is not metered. Both are set so neither wins.
const NON_METERED: &str = "audio.play_bitrate_non_metered_enumeration";
/// Marks the non-metered level as the one to use, which Soloist writes itself on a fresh account.
const MIGRATED: &str = "audio.play_bitrate_non_metered_migrated";
const NORMALIZE: &str = "audio.normalize_v2";
const CROSSFADE: &str = "audio.crossfade_v2";
const CROSSFADE_TIME: &str = "audio.crossfade.time_v2";

/// What Soloist streams at each level, best first. Every level is a ceiling: Spotify serves
/// the best the account and the track have at or below it, so the lossless one is `up_to`.
pub(super) const QUALITIES: &[Offer] = &[
    Offer {
        quality: Quality::Lossless,
        encoding: Encoding::Flac {
            depth: 24,
            rate: 44_100,
        },
        up_to: true,
    },
    Offer {
        quality: Quality::High,
        encoding: Encoding::Vorbis { kbps: 320 },
        up_to: false,
    },
    Offer {
        quality: Quality::Normal,
        encoding: Encoding::Vorbis { kbps: 160 },
        up_to: false,
    },
    Offer {
        quality: Quality::Low,
        encoding: Encoding::Vorbis { kbps: 96 },
        up_to: false,
    },
];

/// The preferences Sonora decides for Soloist. Soloist is started again when they change, since
/// it reads them only as it starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Prefs {
    pub quality: Quality,
    /// Whether Soloist levels tracks to one loudness, with a peak limiter. It does by default.
    pub normalise: bool,
}

/// The number Soloist's preferences give a level. Only 5, lossless, is confirmed by measuring
/// what Soloist fetches. The lossy ones follow Spotify's own tiers, 96, 160 and 320 kilobits.
pub(super) fn enumeration(quality: Quality) -> u8 {
    match quality.within(QUALITIES) {
        Quality::HiRes | Quality::Lossless => 5,
        Quality::High => 4,
        Quality::Normal => 3,
        Quality::Low => 2,
    }
}

/// `existing` with every key `prefs` decides set, and every other line kept. Soloist's own
/// crossfade is always off, since Sonora decides what follows a track and a fade into the next
/// would move where one ends and the next begins. Line endings come out as `\n`, and merging a
/// merged file changes nothing.
pub fn merge(existing: &str, prefs: Prefs) -> String {
    let level = enumeration(prefs.quality).to_string();
    let wanted = [
        (ENUMERATION, level.as_str()),
        (NON_METERED, level.as_str()),
        (MIGRATED, "true"),
        (NORMALIZE, if prefs.normalise { "true" } else { "false" }),
        (CROSSFADE, "false"),
    ];

    let mut out = String::with_capacity(existing.len() + 160);
    for line in existing.lines().filter(|line| !line.is_empty()) {
        let key = line.split('=').next().unwrap_or_default();
        if key == CROSSFADE_TIME || wanted.iter().any(|(wanted, _)| *wanted == key) {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    for (key, value) in wanted {
        out.push_str(key);
        out.push('=');
        out.push_str(value);
        out.push('\n');
    }
    out
}

/// Whether `existing` already says what `prefs` decides. Soloist rewrites the file in its own
/// order and layout as it runs, so the keys are compared and the text is not.
pub fn holds(existing: &str, prefs: Prefs) -> bool {
    let level = enumeration(prefs.quality).to_string();
    let value = |wanted: &str| {
        existing
            .lines()
            .filter_map(|line| line.split_once('='))
            .rev()
            .find(|(key, _)| key.trim() == wanted)
            .map(|(_, value)| value.trim())
    };
    value(ENUMERATION) == Some(level.as_str())
        && value(NON_METERED) == Some(level.as_str())
        && value(MIGRATED) == Some("true")
        && value(NORMALIZE) == Some(if prefs.normalise { "true" } else { "false" })
        && value(CROSSFADE) == Some("false")
        && value(CROSSFADE_TIME).is_none()
}

/// Writes `prefs` into every preferences file under `data_dir` that Soloist has made and that
/// does not hold them yet, and returns how many files that changed. A file Soloist has not made yet is left alone, which is the
/// account of a first pairing, so [`stale`] tells when it has appeared.
pub fn apply(data_dir: &Path, prefs: Prefs) -> io::Result<usize> {
    let mut changed = 0;
    for path in files(data_dir) {
        let existing = fs::read_to_string(&path)?;
        if !holds(&existing, prefs) {
            replace(&path, merge(&existing, prefs).as_bytes())?;
            changed += 1;
        }
    }
    Ok(changed)
}

/// Whether Soloist has made a preferences file for an account under `data_dir`. It has not on a
/// first pairing, when it makes the file just after it signs in, with its own values, having
/// already read what it needed.
pub fn has_account(data_dir: &Path) -> bool {
    fs::read_dir(data_dir.join("settings").join("Users")).is_ok_and(|accounts| {
        accounts
            .filter_map(Result::ok)
            .any(|account| account.path().join("prefs").is_file())
    })
}

/// Whether a preferences file under `data_dir` does not hold `prefs`. This is how the file of a
/// first pairing is noticed, so Soloist can be started once more to read ours.
pub fn stale(data_dir: &Path, prefs: Prefs) -> bool {
    files(data_dir)
        .into_iter()
        .any(|path| fs::read_to_string(&path).is_ok_and(|existing| !holds(&existing, prefs)))
}

/// The preferences files that exist: the shared one and one for each account.
fn files(data_dir: &Path) -> Vec<PathBuf> {
    let settings = data_dir.join("settings");
    let mut found = vec![settings.join("prefs")];
    if let Ok(accounts) = fs::read_dir(settings.join("Users")) {
        let mut accounts: Vec<PathBuf> = accounts
            .filter_map(Result::ok)
            .map(|account| account.path().join("prefs"))
            .collect();
        accounts.sort();
        found.extend(accounts);
    }
    found.retain(|path| path.is_file());
    found
}

/// Puts `bytes` in place of the file at `path`, through a file beside it that is renamed over
/// the old one, so a crash leaves the old preferences rather than half of the new. The new file
/// keeps the permissions of the old.
fn replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let beside = path.with_extension("sonora");
    fs::write(&beside, bytes)?;
    let finish = fs::metadata(path)
        .and_then(|old| fs::set_permissions(&beside, old.permissions()))
        .and_then(|()| fs::rename(&beside, path));
    if finish.is_err() {
        fs::remove_file(&beside).ok();
    }
    finish
}
