//! Spotify playback through Soloist, Spotify's own headless client.
//!
//! librespot asks Spotify's access point for each track's audio key, and Spotify now refuses that
//! for newer accounts. Soloist is the client Spotify supports for this, so this engine starts it
//! as a child process and drives it over its local WebSocket API instead of decoding anything
//! itself. Sonora still owns the queue: it loads one track at a time and learns from the events
//! here when that track has played, paused, moved or ended.
//!
//! Soloist plays through the system's audio output on its own, so nothing here reaches Sonora's
//! equalizer, spectrum or downloads. Sonora neither bundles nor downloads Soloist: the listener
//! installs a build from Spotify and gives Sonora its path and their own API key.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use async_trait::async_trait;
use futures::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::{Fault, PlaybackConfig, PlaybackEvent, PlaybackEvents, PlaybackFactory, Player};

const TRACK_PREFIX: &str = "spotify:track:";
const API_KEY_VAR: &str = "SONORA_SOLOIST_API_KEY";
const BINARY_VAR: &str = "SONORA_SOLOIST_BIN";
const NAME_VAR: &str = "SONORA_SOLOIST_NAME";
/// The name Soloist shows in Spotify's apps. It differs from the `Sonora` that the Connect
/// worker uses, so the two are never mistaken for one another in a device list.
const DEFAULT_DEVICE_NAME: &str = "Sonora (Soloist)";
/// How long Soloist is left alone after it signs in because the listener picked it in Spotify,
/// which starts the song it had on the device and moves playback to it, before it is taken back.
const RECLAIM_SETTLE: Duration = Duration::from_millis(1500);
/// How long Soloist is given to give up the active device before it is made active again.
const RECLAIM_GAP: Duration = Duration::from_millis(700);
/// How long after one Soloist has gone the next may be started. Both restore the same saved login,
/// and one that is restored while the last is only just gone has been refused.
const SETTLE: Duration = Duration::from_secs(1);
/// How many of Soloist's last lines are kept to say what went wrong when something does.
const TAIL: usize = 50;
/// How long after Soloist goes away its last lines are given to arrive.
const TAIL_SETTLE: Duration = Duration::from_millis(150);
/// Soloist's exit code when its build has passed its 90 days.
const EXPIRED: i32 = 10;
/// Soloist's exit code when it cannot take the data directory, which is what a Soloist left
/// running by an earlier Sonora, or by a crash, makes it do.
const ORPHANED: i32 = 1;
/// How long a track waits for a Soloist that asked to be signed in, which the listener does by
/// picking its device in the Spotify app. The track plays as soon as that is done.
const SIGNIN_WAIT: Duration = Duration::from_secs(60);
/// How long an error line from Soloist is held back, since the line that explains it, a rejected
/// API key say, follows within moments and is the one to tell the listener of.
const ERROR_SETTLE: Duration = Duration::from_millis(400);
/// How long a started Soloist has to publish where its WebSocket API listens.
const START_WAIT: Duration = Duration::from_secs(15);
const START_POLL: Duration = Duration::from_millis(100);
/// How many times a start is tried when Soloist exits at once, which it does while a Soloist
/// that was just stopped still holds the data directory.
const START_TRIES: u8 = 3;
const START_RETRY: Duration = Duration::from_millis(700);
/// How long a load waits for Soloist to report the track, counting from when it was asked to
/// play it, before the track is given up as unavailable.
const LOAD_WAIT: Duration = Duration::from_secs(15);
/// How long a seek waits for the new position before it is reported as done regardless.
const SEEK_WAIT: Duration = Duration::from_secs(2);
/// How long a seek waits for Soloist to say seeking is allowed before it is tried regardless.
/// Soloist refuses a seek with `seek_to_restricted` while the track is still loading.
const SEEK_PATIENCE: Duration = Duration::from_secs(5);
/// How far past a track's length the position may run before the track counts as ended, for a
/// Soloist that reports no change when a track runs out.
const END_GRACE: Duration = Duration::from_secs(3);
/// The least time between two position reports, whatever the app asks for.
const MIN_TICK: Duration = Duration::from_millis(50);
/// Keeps a console window from opening for Soloist on Windows. Soloist is a console program, and
/// a release build of the app has no console of its own for it to share.
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// How to start Soloist.
#[derive(Clone, PartialEq, Eq)]
pub struct SoloistConfig {
    /// The `soloist` executable, a path or a name found on `PATH`.
    pub binary: PathBuf,
    /// The listener's own Soloist API key from Spotify for Developers. It is a secret.
    pub api_key: String,
    /// The name the device shows in Spotify's apps.
    pub device_name: String,
    /// Where Soloist keeps its device identity and the session the listener paired it with.
    pub data_dir: PathBuf,
    /// Where Soloist keeps what it can fetch again.
    pub cache_dir: PathBuf,
}

impl std::fmt::Debug for SoloistConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SoloistConfig")
            .field("binary", &self.binary)
            .field("api_key", &"<redacted>")
            .field("device_name", &self.device_name)
            .field("data_dir", &self.data_dir)
            .field("cache_dir", &self.cache_dir)
            .finish()
    }
}

impl SoloistConfig {
    pub fn new(binary: PathBuf, api_key: String, device_name: String) -> Self {
        Self {
            binary,
            api_key,
            device_name,
            data_dir: crate::credentials::dir("spotify-soloist"),
            cache_dir: crate::credentials::root().join("soloist"),
        }
    }

    /// The configuration for an executable and an API key typed in the settings, each falling
    /// back to its environment variable while blank. `None` while there is no API key from
    /// either, which leaves Spotify on librespot.
    pub fn resolve(binary: &str, api_key: &str) -> Option<Self> {
        let given = |value: &str| Some(value.trim().to_owned()).filter(|value| !value.is_empty());
        let from_env = |name: &str| std::env::var(name).ok().and_then(|value| given(&value));

        let api_key = given(api_key).or_else(|| from_env(API_KEY_VAR))?;
        // The path from the environment is taken as the system gives it, which need not be UTF-8.
        let binary = given(binary).map(PathBuf::from).unwrap_or_else(|| {
            std::env::var_os(BINARY_VAR)
                .filter(|binary| !binary.is_empty())
                .map_or_else(|| PathBuf::from("soloist"), PathBuf::from)
        });
        let device_name = from_env(NAME_VAR).unwrap_or_else(|| DEFAULT_DEVICE_NAME.to_owned());
        Some(Self::new(binary, api_key, device_name))
    }
}

/// The executable and API key the settings hold, as typed.
static SETTING: Mutex<(String, String)> = Mutex::new((String::new(), String::new()));

/// Records the settings' executable and API key, and returns whether doing so changed the
/// configuration Soloist would be started with. Typing in a field that changes nothing, such as
/// a path while there is no key, is no change.
pub fn set(binary: &str, api_key: &str) -> bool {
    let before = current();
    *SETTING.lock().unwrap_or_else(PoisonError::into_inner) =
        (binary.trim().to_owned(), api_key.trim().to_owned());
    before != current()
}

/// How to start Soloist now, or `None` while there is no API key, in the settings or the
/// environment, which leaves Spotify on librespot.
pub fn current() -> Option<SoloistConfig> {
    let (binary, api_key) = SETTING
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    SoloistConfig::resolve(&binary, &api_key)
}

/// Builds a Soloist engine on the runtime the factory was made on.
pub struct Factory {
    config: SoloistConfig,
    runtime: Handle,
}

impl Factory {
    pub fn new(config: SoloistConfig, runtime: Handle) -> Self {
        Self { config, runtime }
    }
}

impl PlaybackFactory for Factory {
    fn start(&self, config: PlaybackConfig) -> (Box<dyn Player>, Box<dyn PlaybackEvents>) {
        let (commands, command_rx) = unbounded_channel();
        let (events, event_rx) = unbounded_channel();
        self.runtime.spawn(drive(
            self.config.clone(),
            config.gain,
            config.position_interval,
            command_rx,
            events,
        ));
        (Box::new(Engine { commands }), Box::new(Events(event_rx)))
    }
}

/// The transport the app drives. Every call queues a command for the task that owns Soloist.
pub struct Engine {
    commands: UnboundedSender<Order>,
}

impl Engine {
    fn send(&self, command: Order) -> Result<()> {
        self.commands
            .send(command)
            .map_err(|_| anyhow!("the soloist driver stopped"))
    }
}

impl Player for Engine {
    fn load(&self, track_id: &str, at: Duration, _seamless: bool) -> Result<()> {
        self.send(Order::Load {
            id: track_id.to_owned(),
            at,
            paused: false,
        })
    }

    fn load_paused_at(&self, track_id: &str, at: Duration) -> Result<()> {
        self.send(Order::Load {
            id: track_id.to_owned(),
            at,
            paused: true,
        })
    }

    /// Soloist has no way to line a track up behind the current one, so there is nothing to do.
    fn preload(&self, _track_id: &str, _segue: bool) -> Result<()> {
        Ok(())
    }

    fn play(&self) {
        self.send(Order::Play).ok();
    }

    fn pause(&self) {
        self.send(Order::Pause).ok();
    }

    fn seek(&self, position: Duration) {
        self.send(Order::Seek(position)).ok();
    }

    fn set_gain(&self, gain: f32) {
        self.send(Order::Gain(gain)).ok();
    }
}

/// What the engine's events are read from.
pub struct Events(UnboundedReceiver<PlaybackEvent>);

#[async_trait]
impl PlaybackEvents for Events {
    async fn next(&mut self) -> Option<PlaybackEvent> {
        self.0.recv().await
    }
}

/// What the engine asks of the task that owns Soloist.
#[derive(Debug)]
enum Order {
    Load {
        id: String,
        at: Duration,
        paused: bool,
    },
    Play,
    Pause,
    Seek(Duration),
    Gain(f32),
}

// ---------------------------------------------------------------------------------------------
// What Soloist says.
// ---------------------------------------------------------------------------------------------

/// A message from Soloist's WebSocket API. Events this engine has no use for read as `Other`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Incoming {
    AuthState {
        logged_in: bool,
        #[serde(default)]
        is_active: bool,
    },
    PlaybackState {
        status: Status,
        #[serde(default)]
        item: Option<Entity>,
        #[serde(default)]
        position: Option<Anchor>,
        #[serde(default)]
        is_active: bool,
        /// What can be done right now. Soloist refuses a seek unless it lists `seek`.
        #[serde(default)]
        available_actions: Option<std::collections::HashMap<String, serde_json::Value>>,
    },
    TrackChanged {
        #[serde(default)]
        item: Option<Entity>,
    },
    PlaybackChanged {
        status: Status,
    },
    PositionSync {
        position: Anchor,
    },
    DeviceChanged {
        is_active: bool,
    },
    Error {
        #[serde(default)]
        message: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Status {
    Idle,
    Playing,
    Paused,
    Buffering,
    #[serde(other)]
    Unknown,
}

/// The part of Soloist's entity envelope this engine reads: what it is and how long it runs.
#[derive(Clone, Debug, Default, Deserialize)]
struct Entity {
    #[serde(default)]
    uri: String,
    #[serde(default)]
    decorations: Decorations,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct Decorations {
    #[serde(default)]
    playback: PlaybackInfo,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PlaybackInfo {
    #[serde(default)]
    duration_ms: f64,
}

impl Entity {
    fn duration(&self) -> Option<Duration> {
        let millis = self.decorations.playback.duration_ms;
        (millis.is_finite() && millis > 0.0).then(|| Duration::from_millis(millis as u64))
    }
}

/// A position and the wall-clock time it was true at, which the position since is worked out
/// from.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
struct Anchor {
    position_ms: f64,
    timestamp_ms: f64,
    #[serde(default = "normal_speed")]
    speed: f64,
}

fn normal_speed() -> f64 {
    1.0
}

impl Anchor {
    /// Where the track is at `epoch_ms`, in Unix milliseconds. A speed of zero means it is not
    /// moving, and a clock that reads earlier than the anchor counts as no time passed.
    fn at(&self, epoch_ms: u64) -> Duration {
        let moved = match self.speed > 0.0 {
            true => (epoch_ms as f64 - self.timestamp_ms).max(0.0) * self.speed,
            false => 0.0,
        };
        let millis = (self.position_ms.max(0.0) + moved).max(0.0);
        Duration::from_millis(millis as u64)
    }
}

// ---------------------------------------------------------------------------------------------
// What Soloist is told.
// ---------------------------------------------------------------------------------------------

/// A command for Soloist's WebSocket API.
#[derive(Debug, PartialEq)]
enum Out {
    Play(Option<String>),
    Pause,
    Seek(u64),
    Volume(u8),
    Activate,
    Deactivate,
    GetState,
}

impl Out {
    fn json(&self) -> String {
        use serde_json::json;
        let value = match self {
            Self::Play(None) => json!({ "type": "command", "command": "play" }),
            Self::Play(Some(uri)) => json!({ "type": "command", "command": "play", "uri": uri }),
            Self::Pause => json!({ "type": "command", "command": "pause" }),
            Self::Seek(millis) => {
                json!({ "type": "command", "command": "seek", "position_ms": millis })
            }
            Self::Volume(volume) => {
                json!({ "type": "command", "command": "set_volume", "volume": volume })
            }
            Self::Activate => json!({ "type": "command", "command": "activate" }),
            Self::Deactivate => json!({ "type": "command", "command": "deactivate" }),
            Self::GetState => json!({ "type": "command", "command": "get_state" }),
        };
        value.to_string()
    }
}

/// Taking Soloist back after the Spotify app that signed it in has started its own song on it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Reclaim {
    /// Waiting for what the app does to settle, until then.
    Settle(Instant),
    /// Soloist was told to give up the active device, and is made active again at then.
    Reactivate(Instant),
}

/// What the tracker decides: a command to send Soloist, or an event for the app.
#[derive(Debug, PartialEq)]
enum Action {
    Send(Out),
    Emit(PlaybackEvent),
}

// ---------------------------------------------------------------------------------------------
// The track the app asked for, and what became of it.
// ---------------------------------------------------------------------------------------------

/// The one track the app has asked Soloist to play.
#[derive(Debug)]
struct Wanted {
    id: String,
    uri: String,
    /// Where the track starts, and while it is still loading, where a seek moves that to.
    at: Duration,
    /// The track was loaded to wait paused. Playing is not reported until it has paused.
    hold: bool,
    /// Soloist has been told to play it.
    sent: bool,
    /// Soloist has reported it as the current item.
    seen: bool,
    /// It has ended, failed or been replaced, so nothing more is reported for it.
    done: bool,
    /// The last of playing and paused that was reported, so one is reported once.
    reported: Option<Status>,
    duration: Option<Duration>,
    /// A seek that has been sent, and when it is reported done whether or not Soloist says.
    seeking: Option<(Duration, Instant)>,
    /// A seek that waits for Soloist to allow it, and the time after which it is sent anyway.
    pending_seek: Option<PendingSeek>,
    seek_until: Option<Instant>,
    /// The seek last sent, to send again if Soloist turns out not to have allowed it yet.
    last_seek: Option<PendingSeek>,
    /// When the track is given up as unavailable if Soloist has not reported it by then.
    deadline: Option<Instant>,
}

/// A seek not yet sent. `report` is whether the app asked for it, and so is told when it is done:
/// the seek that puts a track at its starting place is not the app's.
#[derive(Clone, Copy, Debug)]
struct PendingSeek {
    at: Duration,
    report: bool,
}

impl Wanted {
    /// Takes the waiting seek as the command that sends it, starting the clock on reporting it.
    fn take_seek(&mut self, now: Instant) -> Option<Action> {
        let seek = self.pending_seek.take()?;
        self.last_seek = Some(seek);
        self.seek_until = None;
        if seek.report {
            self.seeking = Some((seek.at, now + SEEK_WAIT));
        }
        Some(Action::Send(Out::Seek(millis(seek.at))))
    }
}

/// What Soloist is doing, kept as the app asked for it. It holds no I/O: messages and commands
/// go in and the actions to take come out, so every path is tested without a Soloist.
#[derive(Debug)]
struct Tracker {
    device: String,
    logged_in: bool,
    active: bool,
    status: Status,
    /// Whether Soloist last listed `seek` among what can be done.
    can_seek: bool,
    /// The last item Soloist reported, whatever asked for it.
    current: Option<String>,
    anchor: Option<Anchor>,
    wanted: Option<Wanted>,
    volume: u8,
    /// Soloist said it waits for a login, and the listener has been told to pick it in Spotify.
    login_prompted: bool,
    /// Soloist said its API key is not valid, and the listener has been told.
    key_rejected: bool,
    /// The first error line Soloist printed that has not been told yet, and when.
    error_seen: Option<(Instant, String)>,
    /// An error line of Soloist's has been told to the listener already.
    error_reported: bool,
    /// Soloist waited for the listener to pick it in Spotify, so its next login is theirs.
    paired_by_user: bool,
    reclaim: Option<Reclaim>,
}

impl Tracker {
    fn new(device: &str, gain: f32) -> Self {
        Self {
            device: device.to_owned(),
            logged_in: false,
            active: false,
            status: Status::Idle,
            can_seek: false,
            current: None,
            anchor: None,
            wanted: None,
            volume: percent(gain),
            login_prompted: false,
            key_rejected: false,
            error_seen: None,
            error_reported: false,
            paired_by_user: false,
            reclaim: None,
        }
    }

    /// Plays the track that was waiting for Soloist to be ready, if there is one.
    fn replay(&mut self, now: Instant, out: &mut Vec<Action>) {
        if self
            .wanted
            .as_ref()
            .is_some_and(|wanted| !wanted.sent && !wanted.done)
        {
            self.send_play(now, out);
        }
    }

    /// Gives up on the track that is still starting, which fails it now rather than when its
    /// time runs out.
    fn give_up(&mut self) -> Vec<Action> {
        let mut out = Vec::new();
        if let Some(wanted) = self
            .wanted
            .as_mut()
            .filter(|wanted| !wanted.seen && !wanted.done)
        {
            wanted.done = true;
            wanted.deadline = None;
            out.push(Action::Emit(PlaybackEvent::Unavailable {
                id: Some(wanted.id.clone()),
            }));
        }
        out
    }

    /// A line Soloist printed. It says why it will not sign in or play only in its log, so the
    /// lines that explain are read here and the rest are left to the log.
    fn on_line(&mut self, line: &str, now: Instant) -> Vec<Action> {
        let mut out = Vec::new();
        let Some((level, message)) = read_line(line) else {
            return out;
        };
        let lower = message.to_lowercase();
        if lower.contains("-> logging_in") {
            // A login is under way, from a restored session or from the listener picking the
            // device, so what was said of the last one no longer holds.
            self.key_rejected = false;
            self.login_prompted = false;
        } else if lower.contains("--api-key") || lower.contains("oauth_invalid_credentials") {
            if !std::mem::replace(&mut self.key_rejected, true) {
                log::warn!("playback: soloist does not accept the API key: {message}");
                out.push(Action::Emit(fault(Fault::InvalidKey)));
                out.extend(self.give_up());
            }
        } else if lower.starts_with("waiting for login") {
            if !self.key_rejected && !std::mem::replace(&mut self.login_prompted, true) {
                log::warn!(
                    "playback: soloist waits for a login; open Spotify and pick the device \
                     {:?} to pair it",
                    self.device
                );
                out.push(Action::Emit(fault(Fault::NotSignedIn(self.device.clone()))));
                self.paired_by_user = true;
                // The listener has to do something, so the track is left waiting for them.
                if let Some(wanted) = self
                    .wanted
                    .as_mut()
                    .filter(|wanted| !wanted.seen && !wanted.done)
                {
                    wanted.deadline = Some(now + SIGNIN_WAIT);
                }
            }
        } else if level == 'E' && !self.key_rejected && self.error_seen.is_none() {
            self.error_seen = Some((now, message.to_owned()));
        }
        out
    }

    /// The load to make again on a fresh Soloist, if the track was still starting when this one
    /// went away.
    fn retry_order(&self) -> Option<Order> {
        let wanted = self
            .wanted
            .as_ref()
            .filter(|wanted| !wanted.seen && !wanted.done)?;
        Some(Order::Load {
            id: wanted.id.clone(),
            at: wanted.at,
            paused: wanted.hold,
        })
    }

    /// The app's commands.
    fn on_command(&mut self, command: Order, now: Instant) -> Vec<Action> {
        let mut out = Vec::new();
        match command {
            Order::Load { id, at, paused } => {
                let uri = format!("{TRACK_PREFIX}{id}");
                out.push(Action::Emit(PlaybackEvent::Loading {
                    id: Some(id.clone()),
                    at,
                }));
                self.anchor = None;
                // What was allowed of the last track says nothing of this one.
                self.can_seek = false;
                self.wanted = Some(Wanted {
                    id,
                    uri,
                    at,
                    hold: paused,
                    sent: false,
                    seen: false,
                    done: false,
                    reported: None,
                    duration: None,
                    seeking: None,
                    pending_seek: None,
                    seek_until: None,
                    last_seek: None,
                    deadline: Some(now + LOAD_WAIT),
                });
                match self.logged_in {
                    true => self.send_play(now, &mut out),
                    // A Soloist that has said it cannot be used is not waited on again, since
                    // nothing has changed. The listener is told again, and the track fails.
                    false if self.key_rejected => {
                        out.push(Action::Emit(fault(Fault::InvalidKey)));
                        out.extend(self.give_up());
                    }
                    false if self.login_prompted => {
                        out.push(Action::Emit(fault(Fault::NotSignedIn(self.device.clone()))));
                        out.extend(self.give_up());
                    }
                    false => log::info!(
                        "playback: soloist is not signed in yet, so it is waiting; if this \
                         lasts, open Spotify and pick the device {:?} once to pair it",
                        self.device
                    ),
                }
            }
            Order::Play => {
                if self.logged_in {
                    out.push(Action::Send(Out::Play(None)));
                }
            }
            Order::Pause => {
                if self.logged_in {
                    out.push(Action::Send(Out::Pause));
                }
            }
            Order::Seek(position) => {
                if let Some(wanted) = self.wanted.as_mut().filter(|wanted| !wanted.done) {
                    match wanted.seen {
                        // Still loading, so the track starts from the new place instead.
                        false => wanted.at = position,
                        true => {
                            wanted.pending_seek = Some(PendingSeek {
                                at: position,
                                report: true,
                            });
                            wanted.seek_until = Some(now + SEEK_PATIENCE);
                        }
                    }
                }
                self.try_seek(now, &mut out);
            }
            Order::Gain(gain) => {
                self.volume = percent(gain);
                if self.logged_in {
                    out.push(Action::Send(Out::Volume(self.volume)));
                }
            }
        }
        out
    }

    /// A message from Soloist.
    fn on_message(&mut self, message: Incoming, now: Instant, epoch_ms: u64) -> Vec<Action> {
        let mut out = Vec::new();
        match message {
            Incoming::AuthState {
                logged_in,
                is_active,
            } => {
                let was = std::mem::replace(&mut self.logged_in, logged_in);
                self.active = is_active;
                if logged_in && !was {
                    log::info!("playback: soloist is signed in");
                    out.push(Action::Send(Out::Volume(self.volume)));
                    match std::mem::take(&mut self.paired_by_user) {
                        // The app that signed Soloist in starts its own song on it at once,
                        // which a track asked for at the same moment gets lost in.
                        true => self.reclaim = Some(Reclaim::Settle(now + RECLAIM_SETTLE)),
                        false => self.replay(now, &mut out),
                    }
                }
            }
            Incoming::PlaybackState {
                status,
                item,
                position,
                is_active,
                available_actions,
            } => {
                self.active = is_active;
                if let Some(actions) = available_actions {
                    self.can_seek = actions.contains_key("seek");
                }
                if let Some(item) = item.filter(|item| !item.uri.is_empty()) {
                    self.observe_item(&item, now, &mut out);
                }
                if let Some(position) = position
                    && self.is_wanted_playing()
                {
                    self.anchor = Some(position);
                }
                self.observe_status(status, epoch_ms, &mut out);
                self.try_seek(now, &mut out);
            }
            Incoming::TrackChanged { item } => {
                if let Some(item) = item.filter(|item| !item.uri.is_empty()) {
                    self.observe_item(&item, now, &mut out);
                }
            }
            Incoming::PlaybackChanged { status } => {
                self.observe_status(status, epoch_ms, &mut out);
            }
            Incoming::PositionSync { position } => {
                if self.is_wanted_playing() {
                    self.anchor = Some(position);
                    let at = position.at(epoch_ms);
                    if let Some(wanted) = self.wanted.as_mut()
                        && wanted.seeking.take().is_some()
                    {
                        out.push(Action::Emit(PlaybackEvent::Seeked {
                            id: Some(wanted.id.clone()),
                            at,
                        }));
                    }
                }
            }
            Incoming::DeviceChanged { is_active } => self.active = is_active,
            // Soloist refuses a command for now while it is between tracks, which it says is
            // restricted. That passes, so it is not told to the listener and the track is not
            // given up on. A seek it refused is put back to be sent when it is allowed.
            Incoming::Error { message } if message.to_lowercase().contains("restricted") => {
                log::info!("playback: soloist refused a command for now: {message}");
                if message.to_lowercase().contains("seek_to_restricted") {
                    self.can_seek = false;
                    if let Some(wanted) = self.wanted.as_mut().filter(|wanted| !wanted.done)
                        && let Some(seek) = wanted.last_seek.take()
                    {
                        wanted.pending_seek = Some(seek);
                        wanted.seek_until = Some(now + SEEK_PATIENCE);
                    }
                }
            }
            Incoming::Error { message } => {
                log::warn!("playback: soloist reports an error: {message}");
                out.push(Action::Emit(fault(Fault::Reported(message))));
                if let Some(wanted) = self
                    .wanted
                    .as_mut()
                    .filter(|wanted| wanted.sent && !wanted.seen && !wanted.done)
                {
                    wanted.done = true;
                    wanted.deadline = None;
                    out.push(Action::Emit(PlaybackEvent::Unavailable {
                        id: Some(wanted.id.clone()),
                    }));
                }
            }
            Incoming::Other => {}
        }
        out
    }

    /// The clock. It reports the position while the track plays, finishes a seek Soloist never
    /// answered, ends a track that ran out unannounced and gives up on one that never started.
    fn on_tick(&mut self, now: Instant, epoch_ms: u64) -> Vec<Action> {
        let mut out = Vec::new();
        if self
            .error_seen
            .as_ref()
            .is_some_and(|(at, _)| now.duration_since(*at) >= ERROR_SETTLE)
        {
            if let Some((_, message)) = self.error_seen.take()
                && !self.key_rejected
                && !std::mem::replace(&mut self.error_reported, true)
            {
                log::warn!("playback: soloist reports an error: {message}");
                out.push(Action::Emit(fault(Fault::Reported(message))));
            }
        }
        match self.reclaim {
            Some(Reclaim::Settle(at)) if now >= at => match self.active {
                true => {
                    log::info!(
                        "playback: the app that signed soloist in made it the active device, so \
                         that is given up before playing here"
                    );
                    out.push(Action::Send(Out::Deactivate));
                    self.reclaim = Some(Reclaim::Reactivate(now + RECLAIM_GAP));
                }
                false => {
                    self.reclaim = None;
                    self.replay(now, &mut out);
                }
            },
            Some(Reclaim::Reactivate(at)) if now >= at => {
                self.reclaim = None;
                self.active = false;
                self.replay(now, &mut out);
            }
            _ => {}
        }
        let Self {
            wanted,
            anchor,
            status,
            device,
            logged_in,
            can_seek,
            ..
        } = self;
        let Some(wanted) = wanted.as_mut().filter(|wanted| !wanted.done) else {
            return out;
        };

        if !wanted.seen {
            if wanted.deadline.is_some_and(|deadline| now >= deadline) {
                match logged_in {
                    true => {
                        log::warn!("playback: soloist never started {}", wanted.uri);
                        out.push(Action::Emit(fault(Fault::NeverStarted)));
                    }
                    false => {
                        log::warn!(
                            "playback: soloist is not signed in; open Spotify and pick the \
                             device {device:?} to pair it"
                        );
                        out.push(Action::Emit(fault(Fault::NotSignedIn(device.clone()))));
                    }
                }
                wanted.done = true;
                wanted.deadline = None;
                out.push(Action::Emit(PlaybackEvent::Unavailable {
                    id: Some(wanted.id.clone()),
                }));
            }
            return out;
        }

        if wanted.pending_seek.is_some() {
            let out_of_patience = wanted.seek_until.is_some_and(|until| now >= until);
            if *can_seek || out_of_patience {
                out.extend(wanted.take_seek(now));
            } else {
                // Soloist only lists what is allowed when it sends the whole state, which it
                // does not do on its own after a track starts.
                out.push(Action::Send(Out::GetState));
            }
        }

        if let Some((at, until)) = wanted.seeking
            && now >= until
        {
            wanted.seeking = None;
            out.push(Action::Emit(PlaybackEvent::Seeked {
                id: Some(wanted.id.clone()),
                at,
            }));
        }

        if *status == Status::Playing
            && !wanted.hold
            && let Some(anchor) = anchor
        {
            let at = anchor.at(epoch_ms);
            match wanted.duration {
                Some(length) if at >= length + END_GRACE => {
                    wanted.done = true;
                    out.push(Action::Emit(PlaybackEvent::Ended {
                        id: Some(wanted.id.clone()),
                    }));
                    out.push(Action::Send(Out::Pause));
                }
                _ => out.push(Action::Emit(PlaybackEvent::Position {
                    id: Some(wanted.id.clone()),
                    at,
                })),
            }
        }
        out
    }

    /// What is left when Soloist goes away: the track being played is over, and one that was
    /// still loading will not load.
    fn on_lost(&mut self) -> Vec<Action> {
        let mut out = Vec::new();
        self.logged_in = false;
        if let Some(wanted) = self.wanted.as_mut().filter(|wanted| !wanted.done) {
            wanted.done = true;
            out.push(Action::Emit(PlaybackEvent::Unavailable {
                id: Some(wanted.id.clone()),
            }));
        }
        out
    }

    /// Sends the waiting seek if Soloist allows seeking now. Soloist answers a seek it does not
    /// allow with `seek_to_restricted`, so it waits for `on_tick` to ask for the state instead.
    fn try_seek(&mut self, now: Instant, out: &mut Vec<Action>) {
        if !self.can_seek {
            return;
        }
        if let Some(wanted) = self
            .wanted
            .as_mut()
            .filter(|wanted| wanted.seen && !wanted.done)
            && let Some(seek) = wanted.take_seek(now)
        {
            out.push(seek);
        }
    }

    fn is_wanted_playing(&self) -> bool {
        self.wanted
            .as_ref()
            .is_some_and(|wanted| wanted.seen && !wanted.done)
    }

    /// Tells Soloist to play the wanted track, taking over as the active device first when it
    /// is not, and starts the clock on how long that may take.
    ///
    /// When Soloist is already on that very track it may report nothing new, so the track is
    /// taken as seen at once and put back where it was asked to start.
    fn send_play(&mut self, now: Instant, out: &mut Vec<Action>) {
        let Some(wanted) = self.wanted.as_mut() else {
            return;
        };
        // What was allowed of the track before says nothing of the one Soloist is about to load,
        // which it may be doing again for the very same track.
        self.can_seek = false;
        if !self.active {
            out.push(Action::Send(Out::Activate));
        }
        out.push(Action::Send(Out::Play(Some(wanted.uri.clone()))));
        wanted.sent = true;
        wanted.deadline = Some(now + LOAD_WAIT);

        if self.current.as_deref() == Some(wanted.uri.as_str()) {
            let at = wanted.at;
            Self::start(wanted, None, true, now, out);
            let status = self.status;
            // The track is already the current one, so it will report no change when it
            // restarts. Say what is true now rather than wait for it.
            if wanted.hold {
                wanted.hold = false;
                wanted.reported = Some(Status::Paused);
                out.push(Action::Emit(PlaybackEvent::Paused {
                    id: Some(wanted.id.clone()),
                    at,
                }));
            } else if status == Status::Playing {
                wanted.reported = Some(Status::Playing);
                out.push(Action::Emit(PlaybackEvent::Playing {
                    id: Some(wanted.id.clone()),
                    at,
                }));
            }
        }
    }

    /// Soloist reported `item` as the current one.
    fn observe_item(&mut self, item: &Entity, now: Instant, out: &mut Vec<Action>) {
        self.current = Some(item.uri.clone());
        let Some(wanted) = self.wanted.as_mut().filter(|wanted| !wanted.done) else {
            return;
        };
        if item.uri == wanted.uri {
            if !wanted.seen {
                Self::start(wanted, item.duration(), false, now, out);
            } else if wanted.duration.is_none()
                && let Some(length) = item.duration()
            {
                wanted.duration = Some(length);
                out.push(Action::Emit(PlaybackEvent::Length {
                    id: Some(wanted.id.clone()),
                    duration: length,
                }));
            }
        } else if wanted.seen {
            // Something else is playing, which is how a track that ran out looks when Soloist
            // goes on to a track of its own. Silence it: the app chooses what comes next.
            wanted.done = true;
            out.push(Action::Emit(PlaybackEvent::Ended {
                id: Some(wanted.id.clone()),
            }));
            out.push(Action::Send(Out::Pause));
        }
    }

    /// The wanted track is now the one Soloist has. It starts where it was asked to, and waits
    /// paused if that is how it was loaded. `known` is whether it was playing before it was
    /// asked for, so that a start from 0 still sends the seek that puts it back. The seek waits
    /// until Soloist allows it, which it does not while the track is still loading.
    fn start(
        wanted: &mut Wanted,
        length: Option<Duration>,
        known: bool,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        wanted.seen = true;
        wanted.deadline = None;
        if let Some(length) = length {
            wanted.duration = Some(length);
            out.push(Action::Emit(PlaybackEvent::Length {
                id: Some(wanted.id.clone()),
                duration: length,
            }));
        }
        if known || !wanted.at.is_zero() {
            wanted.pending_seek = Some(PendingSeek {
                at: wanted.at,
                report: false,
            });
            wanted.seek_until = Some(now + SEEK_PATIENCE);
        }
        if wanted.hold {
            out.push(Action::Send(Out::Pause));
        }
    }

    /// Soloist's status changed.
    fn observe_status(&mut self, status: Status, epoch_ms: u64, out: &mut Vec<Action>) {
        self.status = status;
        let Self { wanted, anchor, .. } = self;
        let Some(wanted) = wanted.as_mut().filter(|wanted| wanted.seen && !wanted.done) else {
            return;
        };
        let at = anchor.map_or(wanted.at, |anchor| anchor.at(epoch_ms));
        let id = Some(wanted.id.clone());
        match status {
            Status::Playing if !wanted.hold && wanted.reported != Some(Status::Playing) => {
                wanted.reported = Some(Status::Playing);
                out.push(Action::Emit(PlaybackEvent::Playing { id, at }));
            }
            Status::Paused if wanted.reported != Some(Status::Paused) => {
                wanted.hold = false;
                wanted.reported = Some(Status::Paused);
                out.push(Action::Emit(PlaybackEvent::Paused { id, at }));
            }
            Status::Idle => {
                wanted.done = true;
                out.push(Action::Emit(PlaybackEvent::Ended { id }));
            }
            _ => {}
        }
    }
}

/// The volume Soloist takes, 0 to 100, for the app's gain.
///
/// The gain is the volume slider on a decibel taper, while Soloist's volume is the slider's own
/// scale, so the taper is undone here. Passing the gain on as it is would send a slider at 60 as
/// a volume of about 10.
fn percent(gain: f32) -> u8 {
    if !gain.is_finite() {
        return 100;
    }
    if gain <= 0.0 {
        return 0;
    }
    let level = 1.0 + 20.0 * gain.log10() / crate::VOLUME_TAPER_DB;
    (level.clamp(0.0, 1.0) * 100.0).round() as u8
}

fn millis(position: Duration) -> u64 {
    position.as_millis().min(u64::MAX as u128) as u64
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

// ---------------------------------------------------------------------------------------------
// The process and the socket.
// ---------------------------------------------------------------------------------------------

type Socket = tokio_tungstenite::WebSocketStream<TcpStream>;

/// Held by the engine that has a Soloist running, so a new engine waits for the old one's Soloist
/// to be gone before it starts its own, as engines are swapped when a setting changes.
static HOST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
/// When the last Soloist this app ran went away.
static LAST_END: Mutex<Option<std::time::Instant>> = Mutex::new(None);

/// Waits until no other engine has a Soloist running, and a moment after the last one went.
async fn host() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = HOST.lock().await;
    let ended = *LAST_END.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(ended) = ended {
        let wait = SETTLE.saturating_sub(ended.elapsed());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
    guard
}

/// What Soloist printed last, oldest first, with the API key taken out.
type Tail = Arc<Mutex<VecDeque<String>>>;

/// A running Soloist and the socket to it. Dropping it stops the process.
struct Daemon {
    child: Child,
    socket: Socket,
    tail: Tail,
    /// What Soloist prints, line by line, as it does.
    lines: UnboundedReceiver<String>,
    /// Where its WebSocket API listens.
    endpoint: String,
    key: HostKey,
}

/// What a Soloist was started with, so one that is already running is used again only when a new
/// one would have been started the same way.
#[derive(Clone, PartialEq, Eq)]
struct HostKey {
    binary: PathBuf,
    api_key: String,
    device_name: String,
    data_dir: PathBuf,
    cache_dir: PathBuf,
}

impl HostKey {
    fn of(config: &SoloistConfig) -> Self {
        Self {
            binary: config.binary.clone(),
            api_key: config.api_key.clone(),
            device_name: config.device_name.clone(),
            data_dir: config.data_dir.clone(),
            cache_dir: config.cache_dir.clone(),
        }
    }
}

/// A Soloist left running when its engine went away, without the socket to it.
struct Host {
    child: Child,
    tail: Tail,
    lines: UnboundedReceiver<String>,
    endpoint: String,
    key: HostKey,
}

/// What the next engine asks of the Soloist that was left running.
enum Ask {
    /// To have it.
    Take(tokio::sync::oneshot::Sender<Host>),
    /// To have it stopped, and to be told once it has.
    Stop(tokio::sync::oneshot::Sender<()>),
}

/// The Soloist left running, if there is one, and how to ask for it.
struct Parked {
    key: HostKey,
    ask: tokio::sync::oneshot::Sender<Ask>,
}

/// An engine is swapped for a new one when a setting changes, and a Soloist that is started
/// again within seconds of the last has had its saved login refused. So the Soloist outlives its
/// engine, and the next engine takes it over instead of starting its own.
static PARKED: Mutex<Option<Parked>> = Mutex::new(None);
/// How long a Soloist is left running with no engine to use it before it is stopped.
const PARK_FOR: Duration = Duration::from_secs(600);

/// Leaves `host` running for the next engine, in a task that owns it, so it is stopped with the
/// runtime when the app quits, and after a while if no engine wants it.
fn park(host: Host) {
    let (ask, asked) = tokio::sync::oneshot::channel::<Ask>();
    *PARKED.lock().unwrap_or_else(PoisonError::into_inner) = Some(Parked {
        key: host.key.clone(),
        ask,
    });
    tokio::spawn(async move {
        let mut host = host;
        tokio::select! {
            asked = asked => match asked {
                Ok(Ask::Take(reply)) => {
                    reply.send(host).ok();
                }
                Ok(Ask::Stop(done)) => {
                    host.child.kill().await.ok();
                    done.send(()).ok();
                }
                // Another Soloist was left in its place, so nothing will ask for this one.
                Err(_) => {
                    host.child.kill().await.ok();
                }
            },
            _ = tokio::time::sleep(PARK_FOR) => {
                log::info!("playback: stopping the soloist nothing has used for a while");
                host.child.kill().await.ok();
            }
        }
    });
}

/// Takes over the Soloist that was left running, if it is still running and was started the way
/// `config` says, and connects to it afresh, which has it say whether it is signed in.
async fn adopt(config: &SoloistConfig) -> Option<Daemon> {
    let parked = PARKED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()?;
    if parked.key != HostKey::of(config) {
        log::info!("playback: the soloist left running was started otherwise, so it is stopped");
        let (done, stopped) = tokio::sync::oneshot::channel();
        if parked.ask.send(Ask::Stop(done)).is_ok() {
            stopped.await.ok();
        }
        return None;
    }
    let (reply, answer) = tokio::sync::oneshot::channel();
    parked.ask.send(Ask::Take(reply)).ok()?;
    let mut host = answer.await.ok()?;
    if host
        .child
        .try_wait()
        .map_or(true, |status| status.is_some())
    {
        return None;
    }
    let stream = TcpStream::connect(&host.endpoint).await.ok()?;
    let (socket, _) = tokio_tungstenite::client_async(format!("ws://{}", host.endpoint), stream)
        .await
        .ok()?;
    // What it printed while no one was reading is old news.
    while host.lines.try_recv().is_ok() {}
    log::info!("playback: using the soloist that is already running");
    Some(Daemon {
        child: host.child,
        socket,
        tail: host.tail,
        lines: host.lines,
        endpoint: host.endpoint,
        key: host.key,
    })
}

/// Why Soloist could not be started, and the last thing it said about it.
struct Failed {
    error: anyhow::Error,
    said: Option<String>,
}

impl Daemon {
    async fn start(config: &SoloistConfig, volume: u8) -> Result<Self, Failed> {
        if let Some(daemon) = adopt(config).await {
            return Ok(daemon);
        }
        let tail = Tail::default();
        let mut last = anyhow!("soloist was not started");
        for attempt in 0..START_TRIES {
            if attempt > 0 {
                tokio::time::sleep(START_RETRY).await;
            }
            tail.lock().unwrap_or_else(PoisonError::into_inner).clear();
            match Self::try_start(config, volume, &tail).await {
                Ok(daemon) => return Ok(daemon),
                // An expired build or a missing executable does not get better by waiting.
                Err(error) if error.downcast_ref::<Fatal>().is_some() => {
                    return Err(Failed::new(error, &tail).await);
                }
                Err(error) => {
                    // A Soloist that exits at once usually found another one holding its data
                    // directory, which one left running by an earlier Sonora does.
                    if matches!(error.downcast_ref::<Exit>(), Some(Exit(Some(ORPHANED)))) {
                        let stopped = kill_orphans(config).await;
                        log::warn!("playback: soloist exited at once, stopped {stopped} orphan(s)");
                    }
                    last = error;
                }
            }
        }
        Err(Failed::new(last, &tail).await)
    }

    async fn try_start(config: &SoloistConfig, volume: u8, tail: &Tail) -> Result<Self> {
        for dir in [&config.data_dir, &config.cache_dir] {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("cannot create {}", dir.display()))?;
        }
        // Files left by a Soloist that did not get to clean up would point at a dead port.
        for file in ["ws.addr", "ws.port"] {
            std::fs::remove_file(config.data_dir.join(file)).ok();
        }

        // Soloist says next to nothing unless asked, and what it says is what explains why it
        // will not sign in or start a song, so it is always asked and its words are kept.
        let mut command = Command::new(&config.binary);
        command
            .arg("--device-name")
            .arg(&config.device_name)
            .arg("--api-key")
            .arg(&config.api_key)
            .arg("--data-dir")
            .arg(&config.data_dir)
            .arg("--cache-dir")
            .arg(&config.cache_dir)
            .arg("--initial-volume")
            .arg(volume.to_string())
            .arg("--ws")
            .arg("127.0.0.1:0")
            .arg("--verbose")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        command.creation_flags(CREATE_NO_WINDOW);
        let mut child = command.spawn().map_err(|error| {
            anyhow::Error::new(Fatal(
                format!(
                    "cannot start {}: {error}; install Soloist from Spotify and set \
                     {BINARY_VAR} to it",
                    config.binary.display()
                ),
                Fault::Missing(config.binary.display().to_string()),
            ))
        })?;

        // Both streams are read whether or not anyone looks, since a full pipe stops Soloist.
        let (seen, lines) = unbounded_channel();
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(keep(
                stdout,
                tail.clone(),
                config.api_key.clone(),
                seen.clone(),
            ));
        }
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(keep(stderr, tail.clone(), config.api_key.clone(), seen));
        }

        let endpoint = wait_for_endpoint(&config.data_dir, &mut child).await?;
        let stream = TcpStream::connect(&endpoint)
            .await
            .with_context(|| format!("cannot reach soloist at {endpoint}"))?;
        let (socket, _) = tokio_tungstenite::client_async(format!("ws://{endpoint}"), stream)
            .await
            .context("soloist did not accept the WebSocket handshake")?;
        Ok(Self {
            child,
            socket,
            tail: tail.clone(),
            lines,
            endpoint,
            key: HostKey::of(config),
        })
    }
}

impl Failed {
    /// Waits a moment for the last words of a Soloist that has just gone, then picks which to keep.
    async fn new(error: anyhow::Error, tail: &Tail) -> Self {
        tokio::time::sleep(TAIL_SETTLE).await;
        Self {
            error,
            said: said(tail),
        }
    }
}

/// Reads Soloist's output line by line into the log, the tail, and the channel the loop reads
/// Soloist's words from.
async fn keep(
    reader: impl AsyncRead + Unpin,
    tail: Tail,
    key: String,
    seen: UnboundedSender<String>,
) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let mut line = plain(&line);
        if !key.is_empty() {
            line = line.replace(&key, "<api key>");
        }
        if line.trim().is_empty() {
            continue;
        }
        log::debug!("soloist: {line}");
        seen.send(line.clone()).ok();
        let mut tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
        if tail.len() >= TAIL {
            tail.pop_front();
        }
        tail.push_back(line);
    }
}

/// `line` without the colour codes a terminal log carries.
fn plain(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c == '\u{1b}' {
            true => {
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            false => out.push(c),
        }
    }
    out
}

/// The time, level and message of a line of Soloist's log, which reads like
/// `00:01:18.908 I [/f:462      ] ready, device_name=...`.
fn read_line(line: &str) -> Option<(char, &str)> {
    let (time, rest) = line.trim_start().split_once(' ')?;
    if !time.contains(':') {
        return None;
    }
    let (level, rest) = rest.trim_start().split_once(' ')?;
    let mut chars = level.chars();
    let (Some(level), None) = (chars.next(), chars.next()) else {
        return None;
    };
    if !level.is_ascii_uppercase() {
        return None;
    }
    let message = rest.split_once("] ").map_or(rest, |(_, message)| message);
    Some((level, message.trim()))
}

/// The last line Soloist printed that tells of a problem, which is what the listener is shown of
/// why something failed. Errors count, and so do warnings that say a login or a request failed.
/// Most of its warnings are about audio sessions closing and are left out.
fn said(tail: &Tail) -> Option<String> {
    const STRONG: [&str; 6] = [
        "login failed",
        "denied",
        "invalid",
        "failed to",
        "forbidden",
        "unauthor",
    ];
    const NOISE: [&str; 5] = [
        "closeaudiosession",
        "closed without close source",
        "event mode capable",
        "property store",
        "zero sized file",
    ];
    let tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
    let message = tail.iter().rev().find_map(|line| {
        let (level, message) = read_line(line)?;
        let lower = message.to_lowercase();
        if NOISE.iter().any(|noise| lower.contains(noise)) {
            return None;
        }
        let tells = level == 'E' || STRONG.iter().any(|sign| lower.contains(sign));
        tells.then_some(message)
    })?;
    match message.chars().count() > 160 {
        true => Some(format!(
            "{}…",
            message.chars().take(160).collect::<String>()
        )),
        false => Some(message.to_owned()),
    }
}

/// An event for a fault, before what Soloist said about it is added.
fn fault(fault: Fault) -> PlaybackEvent {
    PlaybackEvent::Fault { fault, said: None }
}

/// Adds what Soloist last said to every fault in `actions` that has nothing yet, except the ones
/// that are what it said already.
fn add_said(actions: &mut [Action], tail: &Tail) {
    for action in actions {
        let Action::Emit(PlaybackEvent::Fault {
            fault,
            said: said_by,
        }) = action
        else {
            continue;
        };
        if said_by.is_none() && !matches!(fault, Fault::Reported(_)) {
            *said_by = said(tail);
        }
    }
}

/// A failure that starting again cannot mend, and what the listener is told of it.
#[derive(Debug)]
struct Fatal(String, Fault);

impl std::fmt::Display for Fatal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Fatal {}

/// Soloist exited while it was being started, with this code or, for `None`, from a signal.
#[derive(Debug)]
struct Exit(Option<i32>);

impl std::fmt::Display for Exit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(code) => write!(formatter, "soloist exited with code {code}"),
            None => formatter.write_str("soloist was stopped by a signal"),
        }
    }
}

impl std::error::Error for Exit {}

/// What the listener is told when starting Soloist failed with `error`.
fn fault_of(error: &anyhow::Error) -> Fault {
    if let Some(Fatal(_, fault)) = error.downcast_ref::<Fatal>() {
        return fault.clone();
    }
    match error.downcast_ref::<Exit>() {
        Some(Exit(code)) => Fault::Exited(*code),
        None => Fault::Unreachable,
    }
}

/// Waits for Soloist to write where its WebSocket API listens.
async fn wait_for_endpoint(data_dir: &Path, child: &mut Child) -> Result<String> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().context("cannot watch soloist")? {
            return Err(exited(status.code()));
        }
        if let Some(endpoint) = read_endpoint(data_dir) {
            return Ok(endpoint);
        }
        if started.elapsed() >= START_WAIT {
            bail!("soloist did not open its WebSocket API in time");
        }
        tokio::time::sleep(START_POLL).await;
    }
}

/// The address and port Soloist published, as `host:port`.
fn read_endpoint(data_dir: &Path) -> Option<String> {
    let addr = std::fs::read_to_string(data_dir.join("ws.addr")).ok()?;
    let port = std::fs::read_to_string(data_dir.join("ws.port")).ok()?;
    let (addr, port) = (addr.trim(), port.trim());
    let port: u16 = port.parse().ok().filter(|port| *port != 0)?;
    match addr.is_empty() {
        true => None,
        false if addr.contains(':') && !addr.starts_with('[') => Some(format!("[{addr}]:{port}")),
        false => Some(format!("{addr}:{port}")),
    }
}

/// Why Soloist stopped, in words the listener can act on.
fn exited(code: Option<i32>) -> anyhow::Error {
    match code {
        Some(EXPIRED) => anyhow::Error::new(Fatal(
            "this Soloist build has expired; install a newer one from Spotify".to_owned(),
            Fault::Expired,
        )),
        code => anyhow::Error::new(Exit(code)),
    }
}

/// Stops every Soloist still running on this app's data directory and returns how many it
/// stopped. None of them can be the one this app is driving, because it is only called once that
/// one has exited or been killed. Soloists of another data directory are left alone.
async fn kill_orphans(config: &SoloistConfig) -> usize {
    let dir = config.data_dir.to_string_lossy().into_owned();
    #[cfg(unix)]
    {
        let name = config
            .binary
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let listing = Command::new("ps")
            .args(["-axo", "pid=,command="])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await;
        let Ok(listing) = listing else {
            log::warn!("playback: cannot list processes to look for an orphan soloist");
            return 0;
        };
        let own = std::process::id().to_string();
        let mut stopped = 0;
        for line in String::from_utf8_lossy(&listing.stdout).lines() {
            let line = line.trim_start();
            let Some((pid, command)) = line.split_once(char::is_whitespace) else {
                continue;
            };
            if pid == own
                || !command.contains("--data-dir")
                || !command.contains(&dir)
                || !command.contains(&name)
            {
                continue;
            }
            let killed = Command::new("kill")
                .args(["-KILL", pid])
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;
            if killed.is_ok_and(|status| status.success()) {
                log::warn!("playback: stopped orphan soloist {pid}");
                stopped += 1;
            }
        }
        stopped
    }
    #[cfg(windows)]
    {
        // The directory goes in the environment rather than the script, so no quoting can break it.
        let script = "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and \
            $_.CommandLine.Contains($env:SONORA_SOLOIST_DIR) -and $_.ProcessId -ne $PID } | \
            ForEach-Object { Stop-Process -Id $_.ProcessId -Force; $_.ProcessId }";
        let mut command = Command::new("powershell");
        command
            .args(["-NoProfile", "-NonInteractive", "-Command", script])
            .env("SONORA_SOLOIST_DIR", &dir)
            .stdin(Stdio::null())
            .stderr(Stdio::null());
        command.creation_flags(CREATE_NO_WINDOW);
        match command.output().await {
            Ok(output) => {
                let stopped = String::from_utf8_lossy(&output.stdout).lines().count();
                if stopped > 0 {
                    log::warn!("playback: stopped {stopped} orphan soloist process(es)");
                }
                stopped
            }
            Err(error) => {
                log::warn!("playback: cannot look for an orphan soloist: {error}");
                0
            }
        }
    }
}

/// Why the connected loop stopped.
enum Stop {
    /// The engine was dropped, so nothing is left to serve.
    Closed,
    /// Soloist or the socket went away.
    Lost,
}

/// How a Soloist's run ended.
struct Ended {
    stop: Stop,
    /// The volume last asked for, so the next Soloist starts at it.
    volume: u8,
    /// The load to make again on a fresh Soloist, when this one exited with `ORPHANED` while the
    /// track was still starting and an orphan may be what made it.
    retry: Option<Order>,
}

/// Owns Soloist for as long as the engine lives. It starts the process when the first track is
/// asked for, and starts it again for the next track after it has gone. A Soloist that exits with
/// `ORPHANED` while a track starts is taken to have met an orphan: the orphans are stopped and the
/// track is loaded once more on a new one.
async fn drive(
    config: SoloistConfig,
    gain: f32,
    interval: Duration,
    mut commands: UnboundedReceiver<Order>,
    events: UnboundedSender<PlaybackEvent>,
) {
    let mut volume = percent(gain);
    // A load to make again, and the track the last such retry was for, so one track is retried once.
    let mut again: Option<Order> = None;
    let mut retried: Option<String> = None;
    loop {
        let command = match again.take() {
            Some(command) => command,
            None => match commands.recv().await {
                Some(command) => command,
                None => return,
            },
        };
        let id = match &command {
            Order::Load { id, .. } => id.clone(),
            Order::Gain(gain) => {
                volume = percent(*gain);
                continue;
            }
            // Nothing is running to play, pause or seek.
            _ => continue,
        };

        let _host = host().await;
        let daemon = match Daemon::start(&config, volume).await {
            Ok(daemon) => daemon,
            Err(Failed { error, said }) => {
                log::error!("playback: cannot start soloist: {error:#}");
                let fault = PlaybackEvent::Fault {
                    fault: fault_of(&error),
                    said,
                };
                if events.send(fault).is_err()
                    || events
                        .send(PlaybackEvent::Unavailable { id: Some(id) })
                        .is_err()
                {
                    return;
                }
                continue;
            }
        };

        let mut tracker = Tracker::new(&config.device_name, 0.0);
        tracker.volume = volume;
        let may_retry = retried.as_deref() != Some(id.as_str());
        let ended = serve(
            daemon,
            tracker,
            command,
            interval,
            &mut commands,
            &events,
            may_retry,
        )
        .await;
        match ended {
            Ended {
                stop: Stop::Closed, ..
            } => return,
            Ended {
                volume: last,
                retry,
                ..
            } => {
                volume = last;
                match retry {
                    Some(order) => {
                        retried = Some(id);
                        let stopped = kill_orphans(&config).await;
                        log::warn!(
                            "playback: soloist exited while starting a track, stopped {stopped} \
                             orphan(s) and trying again"
                        );
                        again = Some(order);
                    }
                    None => retried = None,
                }
            }
        }
    }
}

/// Runs one Soloist until it goes away or the engine is dropped. `may_retry` is whether a Soloist
/// that exits with `ORPHANED` while the track starts is to be answered with a retry rather than
/// with a failed track.
async fn serve(
    daemon: Daemon,
    mut tracker: Tracker,
    first: Order,
    interval: Duration,
    commands: &mut UnboundedReceiver<Order>,
    events: &UnboundedSender<PlaybackEvent>,
    may_retry: bool,
) -> Ended {
    let Daemon {
        mut child,
        socket,
        tail,
        mut lines,
        endpoint,
        key,
    } = daemon;
    let (mut sink, mut stream) = socket.split();
    let mut ticker = tokio::time::interval(interval.max(MIN_TICK));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Why Soloist went away, when it did so by itself, and what the listener is told of it.
    let mut exit: Option<Option<i32>> = None;
    let mut fault: Option<Fault> = None;
    let mut actions = tracker.on_command(first, Instant::now());
    let stop = loop {
        add_said(&mut actions, &tail);
        if let Some(stop) = perform(actions, &mut sink, events).await {
            break stop;
        }
        actions = tokio::select! {
            command = commands.recv() => match command {
                Some(command) => tracker.on_command(command, Instant::now()),
                None => break Stop::Closed,
            },
            message = stream.next() => match message {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<Incoming>(&text) {
                    Ok(message) => tracker.on_message(message, Instant::now(), epoch_ms()),
                    Err(error) => {
                        log::debug!("playback: unreadable soloist message: {error}");
                        Vec::new()
                    }
                },
                Some(Ok(Message::Close(_))) | None => {
                    fault = Some(Fault::Unreachable);
                    break Stop::Lost;
                }
                Some(Err(error)) => {
                    log::warn!("playback: soloist socket failed: {error}");
                    fault = Some(Fault::Unreachable);
                    break Stop::Lost;
                }
                Some(Ok(_)) => Vec::new(),
            },
            status = child.wait() => {
                match status {
                    Ok(status) => {
                        let code = status.code();
                        match code {
                            Some(EXPIRED) => {
                                log::error!("playback: {}", exited(Some(EXPIRED)));
                                fault = Some(Fault::Expired);
                            }
                            code => {
                                log::warn!("playback: {}", exited(code));
                                fault = Some(Fault::Exited(code));
                            }
                        }
                        exit = Some(code);
                    }
                    Err(error) => {
                        log::warn!("playback: cannot watch soloist: {error}");
                        fault = Some(Fault::Unreachable);
                    }
                }
                break Stop::Lost;
            },
            _ = ticker.tick() => tracker.on_tick(Instant::now(), epoch_ms()),
            Some(line) = lines.recv() => tracker.on_line(&line, Instant::now()),
        };
    };

    log::info!(
        "playback: the run with soloist ended, closed {}, exit {exit:?}, fault {fault:?}, a track \
         was waiting for it: {}",
        matches!(stop, Stop::Closed),
        tracker.retry_order().is_some()
    );
    // A Soloist that is still running when its engine goes away is left running for the next
    // engine, paused so it does not play on with no one to hear it. One that is gone, or that
    // the app lost touch with, is stopped before anything reads the events, so a track loaded
    // straight away finds the data directory free.
    match matches!(stop, Stop::Closed) && exit.is_none() {
        true => {
            if tracker.status == Status::Playing {
                let pause = Message::text(Out::Pause.json());
                tokio::time::timeout(Duration::from_secs(1), sink.send(pause))
                    .await
                    .ok();
            }
            sink.close().await.ok();
            park(Host {
                child,
                tail: tail.clone(),
                lines,
                endpoint,
                key,
            });
        }
        false => {
            child.kill().await.ok();
            *LAST_END.lock().unwrap_or_else(PoisonError::into_inner) =
                Some(std::time::Instant::now());
        }
    }
    let retry = match (may_retry, exit) {
        (true, Some(Some(ORPHANED))) => tracker.retry_order(),
        _ => None,
    };
    if retry.is_none() {
        if let Some(fault) = fault {
            // Soloist's last words may still be on their way out of its pipes.
            tokio::time::sleep(TAIL_SETTLE).await;
            let said = said(&tail);
            events.send(PlaybackEvent::Fault { fault, said }).ok();
        }
        for action in tracker.on_lost() {
            if let Action::Emit(event) = action {
                events.send(event).ok();
            }
        }
    }
    Ended {
        stop,
        volume: tracker.volume,
        retry,
    }
}

/// Carries out what the tracker decided. Returns why the loop must stop, if it must.
async fn perform(
    actions: Vec<Action>,
    sink: &mut (impl futures::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    events: &UnboundedSender<PlaybackEvent>,
) -> Option<Stop> {
    for action in actions {
        match action {
            Action::Emit(event) => {
                if events.send(event).is_err() {
                    return Some(Stop::Closed);
                }
            }
            Action::Send(command) => {
                if let Err(error) = sink.send(Message::text(command.json())).await {
                    log::warn!("playback: cannot reach soloist: {error}");
                    return Some(Stop::Lost);
                }
            }
        }
    }
    None
}
