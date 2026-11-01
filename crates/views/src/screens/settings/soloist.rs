use std::path::Path;

use gpui::prelude::*;
use gpui::{AnyElement, Context, Entity, PathPromptOptions, Pixels, div, px};
use i18n::t;
use state::{Outcome, Toasts};
use ui::{Button, Input};

use super::{Group, SOLOIST_APPLY_DELAY, SettingsView, Slot};

/// The page of Spotify's developer site where Soloist is downloaded, which has the latest
/// version for each platform, and the release notes.
const SOLOIST_DOWNLOAD: &str = "https://developer.spotify.com/documentation/soloist/reference/downloads-and-updates";

/// The page of Spotify's developer dashboard where a Soloist API key is made.
const SOLOIST_DASHBOARD: &str = "https://developer.spotify.com/dashboard/soloist";

/// How wide both fields are, so the rows line up. A masked field draws a bullet of about 8 pixels
/// for each character, so this leaves room for the 37 characters of an API key and a few over.
const FIELD_WIDTH: Pixels = px(340.);

/// The least width of the button beside a field. Both buttons have it, so they are the same size
/// and the fields start in the same place, and a longer label in another language grows it.
const BUTTON_WIDTH: Pixels = px(120.);

impl SettingsView {
    /// The group that points Spotify's playback at Soloist, Spotify's own headless client. It
    /// stays on the page while Spotify is not signed in, since it is set up beforehand.
    pub(super) fn soloist_groups(&self) -> Vec<Group> {
        vec![
            Group::new("settings-group-spotify-soloist", "icons/speaker.svg")
                .rows([Slot::SoloistDownload, Slot::SoloistBinary, Slot::SoloistKey]),
        ]
    }

    /// Applies the Soloist settings once typing in them has stopped, so playback switches over
    /// by itself when the API key is entered and a path is not retyped into a restart per letter.
    pub(super) fn schedule_soloist(&mut self, cx: &mut Context<Self>) {
        self.soloist_apply = Some(cx.spawn(async move |this, cx| {
            cx.background_executor().timer(SOLOIST_APPLY_DELAY).await;
            this.update(cx, |this, cx| {
                this.playback
                    .update(cx, |playback, cx| playback.apply_soloist(cx));
            })
            .ok();
        }));
    }

    /// The row for the Soloist download link: a button that opens the page where it is downloaded.
    pub(super) fn soloist_download_row(&self, _cx: &mut Context<Self>) -> AnyElement {
        let get = Button::new("soloist-download")
            .label(t!("settings-soloist-download-button"))
            .icon("icons/external-link.svg")
            .small()
            .outline()
            .on_click(|_, _, cx| cx.open_url(SOLOIST_DOWNLOAD));
        self.soloist_message_row("settings-soloist-download", get)
    }

    /// The row for where Soloist is installed: the field, and a button that opens a file dialog
    /// to pick the executable, which is `soloist.exe` on Windows.
    pub(super) fn soloist_binary_row(&self, cx: &mut Context<Self>) -> AnyElement {
        let browse = Button::new("soloist-binary-browse")
            .label(t!("settings-soloist-browse"))
            .small()
            .outline()
            .on_click(cx.listener(|this, _, _, cx| this.browse_soloist(cx)));
        self.soloist_row("settings-soloist-binary", &self.soloist_binary, browse)
    }

    /// The row for the API key: the masked field, and a button that opens the page of Spotify's
    /// developer dashboard where the key is made.
    pub(super) fn soloist_key_row(&self, _cx: &mut Context<Self>) -> AnyElement {
        let get = Button::new("soloist-key-get")
            .label(t!("settings-soloist-get-key"))
            .icon("icons/external-link.svg")
            .small()
            .outline()
            .on_click(|_, _, cx| cx.open_url(SOLOIST_DASHBOARD));
        self.soloist_row("settings-soloist-key", &self.soloist_key, get)
    }

    /// Asks for the Soloist executable and puts its path in the field, which saves it and, once
    /// the delay has passed, switches playback over like typing it would.
    fn browse_soloist(&mut self, cx: &mut Context<Self>) {
        let receiver = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: None,
        });
        cx.spawn(async move |this, cx| {
            let path = match receiver.await {
                Ok(Ok(Some(paths))) => paths.into_iter().next(),
                Ok(Err(error)) => {
                    log::warn!("settings: cannot open the file dialog: {error:#}");
                    cx.update(|cx| Toasts::show(Outcome::Failed, "toast-file-dialog-failed", cx));
                    None
                }
                _ => None,
            };
            let Some(path) = path else {
                return;
            };
            if !is_program(&path, cfg!(windows)) {
                cx.update(|cx| Toasts::show(Outcome::Failed, "toast-soloist-program-invalid", cx));
                return;
            }
            this.update(cx, |this, cx| {
                let path = path.to_string_lossy().into_owned();
                this.soloist_binary
                    .update(cx, |input, cx| input.set_text(path, cx));
            })
            .ok();
        })
        .detach();
    }

    /// A row that holds one of the Soloist fields, which are told apart by `title`, with the
    /// button that helps fill it in beside it.
    fn soloist_row(
        &self,
        title: &'static str,
        field: &Entity<Input>,
        button: Button,
    ) -> AnyElement {
        self.row(
            i18n::lookup(title, None),
            div()
                .flex()
                .items_center()
                .gap_2()
                .child(div().w(FIELD_WIDTH).child(field.clone()))
                .child(button.min_w(BUTTON_WIDTH))
                .into_any_element(),
        )
    }

    fn soloist_message_row(
        &self,
        title: &'static str,
        button: Button,
    ) -> AnyElement {
        self.row(
            i18n::lookup(title, None),
            div()
                .flex()
                .items_center()
                .gap_2()
                .child(button.min_w(BUTTON_WIDTH))
                .into_any_element(),
        )
    }
}

/// Whether a file picked in the dialog can be the Soloist program. The dialog cannot filter by
/// extension, so this does. On Windows the program is an `.exe`. Elsewhere it has no extension
/// to go by, so any file is let through.
fn is_program(path: &Path, windows: bool) -> bool {
    !windows
        || path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"))
}
