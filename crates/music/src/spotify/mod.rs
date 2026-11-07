pub mod auth;

mod albums;
mod artists;
mod client;
mod collection;
mod collection2;
mod connect;
mod links;
mod lyrics;
mod offline;
mod pace;
mod pathfinder;
mod pb;
mod playback;
mod playlists;
mod profiles;
mod radio;
mod search;
mod sink;
mod soloist;
mod soloist_prefs;
mod wire;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::spotify::playback::Factory;
use crate::{
    Capabilities, MusicApi as _, MusicProvider, PlaybackFactory, ProviderSession, Shape,
    SignInFailure, SignInProblem,
};

pub use auth::AuthConfig;
pub use client::LibrespotClient;
pub use lyrics::SpotifyLyrics;
pub use soloist::{SoloistConfig, set as set_soloist};

pub struct SpotifyProvider {
    config: AuthConfig,
}

impl SpotifyProvider {
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }

    pub fn from_env() -> Self {
        Self::new(AuthConfig::from_env())
    }

    fn drop_free(&self, error: anyhow::Error) -> anyhow::Error {
        if matches!(
            error.downcast_ref::<SignInFailure>(),
            Some(SignInFailure(SignInProblem::Premium))
        ) {
            auth::forget(&self.config);
        }
        error
    }

    async fn session(&self, client: LibrespotClient) -> Result<ProviderSession> {
        let profile = client.profile().await?;
        let (playback, capabilities) = self.playback(&client);
        Ok(ProviderSession {
            profile,
            api: Arc::new(client),
            playback,
            shape: Shape::Saved,
            authenticated: true,
            capabilities,
        })
    }

    /// The engine tracks play through, and what it leaves the app to offer. Soloist decodes and
    /// plays by itself, at the level written into its preferences, and keeps no downloads for
    /// Sonora, so those are not offered while it plays. Whether it plays is read again each time an engine starts, so
    /// the settings can change it while the app runs, but what is offered is settled here.
    fn playback(&self, client: &LibrespotClient) -> (Arc<dyn PlaybackFactory>, Capabilities) {
        let runtime = tokio::runtime::Handle::try_current().ok();
        let capabilities = match soloist::current().is_some() && runtime.is_some() {
            true => Capabilities {
                pins: true,
                playlist_covers: true,
                keeps: false,
                downloads: false,
                qualities: soloist_prefs::QUALITIES,
                ..Capabilities::ALL
            },
            false => Capabilities {
                pins: true,
                playlist_covers: true,
                download_gap: playback::DOWNLOAD_GAP,
                qualities: playback::QUALITIES,
                ..Capabilities::ALL
            },
        };
        let factory = Switch {
            librespot: Factory::new(client.session().clone()),
            runtime,
        };
        (Arc::new(factory), capabilities)
    }
}

/// Starts the engine Spotify plays through: Soloist while the settings or the environment give it
/// an API key, librespot otherwise. It decides at each start rather than once, so a setting that
/// changes while the app runs reaches the next engine.
struct Switch {
    librespot: Factory,
    /// What Soloist is run on. Without one it cannot be started, and librespot plays.
    runtime: Option<tokio::runtime::Handle>,
}

impl PlaybackFactory for Switch {
    fn start(
        &self,
        config: crate::PlaybackConfig,
    ) -> (Box<dyn crate::Player>, Box<dyn crate::PlaybackEvents>) {
        match (soloist::current(), &self.runtime) {
            (Some(soloist), Some(runtime)) => {
                log::info!(
                    "spotify: playing through Soloist ({})",
                    soloist.binary.display()
                );
                soloist::Factory::new(soloist, runtime.clone()).start(config)
            }
            _ => self.librespot.start(config),
        }
    }
}

#[async_trait]
impl MusicProvider for SpotifyProvider {
    fn name(&self) -> &'static str {
        "Spotify"
    }

    fn slug(&self) -> &'static str {
        "spotify"
    }

    fn reach(&self) -> Option<String> {
        Some("apresolve.spotify.com".to_owned())
    }

    fn public_art(&self) -> bool {
        true
    }

    fn link(&self, link: &str) -> Option<crate::link::Linked> {
        links::read(link)
    }

    fn sign_in_options(&self) -> Vec<crate::SignIn> {
        vec![crate::SignIn::Default]
    }

    fn abandon(&self) {
        auth::release(&self.config);
    }

    fn stored(&self) -> bool {
        self.config.file().exists()
    }

    /// A session that never connects, which plays downloads and nothing else. It is built on
    /// the caller's tokio runtime.
    fn offline_playback(&self) -> Option<Arc<dyn PlaybackFactory>> {
        if !self.stored() {
            return None;
        }
        let session = auth::session(&self.config)
            .inspect_err(|error| log::warn!("spotify: cannot open an offline session: {error:#}"))
            .ok()?;
        Some(Arc::new(Factory::new(session)))
    }

    async fn restore(&self) -> Result<Option<ProviderSession>> {
        let Some(session) = auth::restore(&self.config)
            .await
            .map_err(|error| self.drop_free(error))?
        else {
            return Ok(None);
        };
        self.session(LibrespotClient::new(session)).await.map(Some)
    }

    async fn sign_in(
        &self,
        _method: crate::SignIn,
        prompt: crate::PromptSink,
        _input: crate::InputSource,
    ) -> Result<ProviderSession> {
        let session = auth::login(&self.config, prompt)
            .await
            .map_err(|error| self.drop_free(error))?;
        self.session(LibrespotClient::new(session)).await
    }

    fn sign_out(&self) {
        auth::forget(&self.config);
    }
}
