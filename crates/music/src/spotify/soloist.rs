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

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use async_trait::async_trait;
use futures::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::{PlaybackConfig, PlaybackEvent, PlaybackEvents, PlaybackFactory, Player};

const TRACK_PREFIX: &str = "spotify:track:";
const API_KEY_VAR: &str = "SONORA_SOLOIST_API_KEY";
const BINARY_VAR: &str = "SONORA_SOLOIST_BIN";
const NAME_VAR: &str = "SONORA_SOLOIST_NAME";
/// The name Soloist shows in Spotify's apps. It differs from the `Sonora` that the Connect
/// worker uses, so the two are never mistaken for one another in a device list.
const DEFAULT_DEVICE_NAME: &str = "Sonora (Soloist)";
/// Soloist's exit code when its build has passed its 90 days.
const EXPIRED: i32 = 10;
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
            Self::GetState => json!({ "type": "command", "command": "get_state" }),
        };
        value.to_string()
    }
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
    /// When the track is given up as unavailable if Soloist has not reported it by then.
    deadline: Option<Instant>,
}

/// A seek not yet sent. `report` is whether the app asked for it, and so is told when it is done:
/// the seek that puts a track at its starting place is not the app's.
#[derive(Debug)]
struct PendingSeek {
    at: Duration,
    report: bool,
}

impl Wanted {
    /// Takes the waiting seek as the command that sends it, starting the clock on reporting it.
    fn take_seek(&mut self, now: Instant) -> Option<Action> {
        let seek = self.pending_seek.take()?;
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
        }
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
                    deadline: Some(now + LOAD_WAIT),
                });
                match self.logged_in {
                    true => self.send_play(now, &mut out),
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
                    if self
                        .wanted
                        .as_ref()
                        .is_some_and(|wanted| !wanted.sent && !wanted.done)
                    {
                        self.send_play(now, &mut out);
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
            Incoming::Error { message } => {
                log::warn!("playback: soloist reports an error: {message}");
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
                    true => log::warn!("playback: soloist never started {}", wanted.uri),
                    false => log::warn!(
                        "playback: soloist is not signed in; open Spotify and pick the device \
                         {device:?} to pair it"
                    ),
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

/// A running Soloist and the socket to it. Dropping it stops the process.
struct Daemon {
    child: Child,
    socket: Socket,
}

impl Daemon {
    async fn start(config: &SoloistConfig, volume: u8) -> Result<Self> {
        let mut last = anyhow!("soloist was not started");
        for attempt in 0..START_TRIES {
            if attempt > 0 {
                tokio::time::sleep(START_RETRY).await;
            }
            match Self::try_start(config, volume).await {
                Ok(daemon) => return Ok(daemon),
                // An expired build or a missing executable does not get better by waiting.
                Err(error) if error.downcast_ref::<Fatal>().is_some() => return Err(error),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    async fn try_start(config: &SoloistConfig, volume: u8) -> Result<Self> {
        for dir in [&config.data_dir, &config.cache_dir] {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("cannot create {}", dir.display()))?;
        }
        // Files left by a Soloist that did not get to clean up would point at a dead port.
        for file in ["ws.addr", "ws.port"] {
            std::fs::remove_file(config.data_dir.join(file)).ok();
        }

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
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        command.creation_flags(CREATE_NO_WINDOW);
        let mut child = command.spawn().map_err(|error| {
            anyhow::Error::new(Fatal(format!(
                "cannot start {}: {error}; install Soloist from Spotify and set \
                 {BINARY_VAR} to it",
                config.binary.display()
            )))
        })?;

        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    log::debug!("soloist: {line}");
                }
            });
        }

        let endpoint = wait_for_endpoint(&config.data_dir, &mut child).await?;
        let stream = TcpStream::connect(&endpoint)
            .await
            .with_context(|| format!("cannot reach soloist at {endpoint}"))?;
        let (socket, _) = tokio_tungstenite::client_async(format!("ws://{endpoint}"), stream)
            .await
            .context("soloist did not accept the WebSocket handshake")?;
        Ok(Self { child, socket })
    }
}

/// A failure that starting again cannot mend.
#[derive(Debug)]
struct Fatal(String);

impl std::fmt::Display for Fatal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Fatal {}

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
        )),
        Some(code) => anyhow!("soloist exited with code {code}"),
        None => anyhow!("soloist was stopped by a signal"),
    }
}

/// Why the connected loop stopped.
enum Stop {
    /// The engine was dropped, so nothing is left to serve.
    Closed,
    /// Soloist or the socket went away.
    Lost,
}

/// Owns Soloist for as long as the engine lives. It starts the process when the first track is
/// asked for, and starts it again for the next track after it has gone.
async fn drive(
    config: SoloistConfig,
    gain: f32,
    interval: Duration,
    mut commands: UnboundedReceiver<Order>,
    events: UnboundedSender<PlaybackEvent>,
) {
    let mut volume = percent(gain);
    loop {
        let Some(command) = commands.recv().await else {
            return;
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

        let daemon = match Daemon::start(&config, volume).await {
            Ok(daemon) => daemon,
            Err(error) => {
                log::error!("playback: cannot start soloist: {error:#}");
                if events
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
        match serve(daemon, tracker, command, interval, &mut commands, &events).await {
            (Stop::Closed, _) => return,
            (Stop::Lost, last) => volume = last,
        }
    }
}

/// Runs one Soloist until it goes away or the engine is dropped. Returns why, and the volume
/// last asked for so the next Soloist starts at it.
async fn serve(
    daemon: Daemon,
    mut tracker: Tracker,
    first: Order,
    interval: Duration,
    commands: &mut UnboundedReceiver<Order>,
    events: &UnboundedSender<PlaybackEvent>,
) -> (Stop, u8) {
    let Daemon { mut child, socket } = daemon;
    let (mut sink, mut stream) = socket.split();
    let mut ticker = tokio::time::interval(interval.max(MIN_TICK));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut actions = tracker.on_command(first, Instant::now());
    let stop = loop {
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
                Some(Ok(Message::Close(_))) | None => break Stop::Lost,
                Some(Err(error)) => {
                    log::warn!("playback: soloist socket failed: {error}");
                    break Stop::Lost;
                }
                Some(Ok(_)) => Vec::new(),
            },
            status = child.wait() => {
                match status {
                    Ok(status) => match status.code() {
                        Some(EXPIRED) => log::error!("playback: {}", exited(Some(EXPIRED))),
                        code => log::warn!("playback: {}", exited(code)),
                    },
                    Err(error) => log::warn!("playback: cannot watch soloist: {error}"),
                }
                break Stop::Lost;
            },
            _ = ticker.tick() => tracker.on_tick(Instant::now(), epoch_ms()),
        };
    };

    // The daemon is stopped before anything reads the events, so a track loaded straight away
    // finds the data directory free.
    child.kill().await.ok();
    for action in tracker.on_lost() {
        if let Action::Emit(event) = action {
            events.send(event).ok();
        }
    }
    (stop, tracker.volume)
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
