//! Channel, group, profile, stream, M3U account and logo endpoints.
//!
//! Split from the transport in [`super`] so the paths and payloads of one resource sit
//! together. Every write here is `IsAdmin` upstream; the reads need a standard user.

use super::*;
use anyhow::{bail, Context, Result};

/// Channels per `edit/bulk` request. The endpoint applies one JSON array in a single
/// transaction, so a whole install would sit under a single timeout; chunks keep each write
/// small enough to retry on its own.
pub const CHANNEL_EDIT_CHUNK_SIZE: usize = 500;

/// Associations per `batch-set-epg` request, for the same reason as
/// [`CHANNEL_EDIT_CHUNK_SIZE`].
const BATCH_SET_EPG_CHUNK_SIZE: usize = 500;

/// What a chunked `batch-set-epg` write achieved (see
/// [`DispatcharrClient::batch_set_epg_outcome`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchEpgOutcome {
    /// Leading associations whose chunk was confirmed upstream (a prefix length).
    pub confirmed: usize,
    /// Channels upstream reports as updated (fewer than `confirmed` is normal: a channel
    /// already on its entry is not an update).
    pub updated: usize,
    /// The first failing chunk's message; `None` when every chunk landed.
    pub failure: Option<String>,
}

/// Channels per `profiles/{id}/channels/bulk-update/` request.
const MEMBERSHIP_CHUNK_SIZE: usize = 500;

/// Whether a failed write was the server rejecting a row (400), which is worth retrying one
/// row at a time; anything else (an outage, a lost answer) would only repeat per row.
fn is_rejected_row(error: &anyhow::Error) -> bool {
    http_status(error) == Some(reqwest::StatusCode::BAD_REQUEST)
}

impl DispatcharrClient {
    /// Every channel, hidden ones included.
    ///
    /// `visibility_filter=all` is not optional: the list endpoint hides channels whose
    /// `hidden_from_output` is set unless it is sent, and the plugin read every channel.
    ///
    /// Every row must carry `streams` (the list serializer always sends it). A row without
    /// the key fails the listing instead of reading as a channel with no streams, because the
    /// stream-list writes replace the whole list.
    pub async fn channels(&self) -> Result<Vec<Channel>> {
        #[derive(Deserialize)]
        struct Listed {
            #[serde(flatten)]
            channel: Channel,
            streams: Vec<i64>,
        }

        let listed: Vec<Listed> = self
            .get_list(
                "/channels/channels/",
                &[("visibility_filter", "all".to_string())],
                "listing channels",
            )
            .await?;
        let channels: Vec<Channel> = listed
            .into_iter()
            .map(|row| Channel {
                streams: row.streams,
                ..row.channel
            })
            .collect();
        for channel in &channels {
            self.channel_cache.insert(channel.id, channel.clone());
        }
        Ok(channels)
    }

    /// Whether channel `id` still exists: `GET /channels/channels/{id}/`, where only a 404
    /// answers `false`. A listing that misses a channel proves nothing (a short page, a
    /// proxy, a scoped read); this does.
    pub async fn channel_exists(&self, id: i64) -> Result<bool> {
        self.exists(&format!("/channels/channels/{id}/"))
            .await
            .with_context(|| format!("checking whether channel {id} exists"))
    }

    /// [`Self::channel_exists`] for a channel group.
    pub async fn group_exists(&self, id: i64) -> Result<bool> {
        self.exists(&format!("/channels/groups/{id}/"))
            .await
            .with_context(|| format!("checking whether channel group {id} exists"))
    }

    /// [`Self::channel_exists`] for a logo.
    pub async fn logo_exists(&self, id: i64) -> Result<bool> {
        self.exists(&format!("/channels/logos/{id}/"))
            .await
            .with_context(|| format!("checking whether logo {id} exists"))
    }

    /// A channel by id, from the cache the last listing filled.
    pub fn channel(&self, id: i64) -> Option<Channel> {
        self.channel_cache.get(&id).map(|entry| entry.clone())
    }

    pub async fn groups(&self) -> Result<Vec<ChannelGroup>> {
        self.get_list("/channels/groups/", &[], "listing channel groups")
            .await
    }

    pub async fn profiles(&self) -> Result<Vec<ChannelProfile>> {
        self.get_list("/channels/profiles/", &[], "listing channel profiles")
            .await
    }

    /// Channel ids enabled in a profile.
    ///
    /// Reads the profile list, which already carries the enabled channel ids; there is no
    /// separate membership endpoint to walk.
    pub async fn profile_channels(&self, profile_id: i64) -> Result<Vec<i64>> {
        let profiles = self.profiles().await?;
        Ok(profiles
            .into_iter()
            .find(|profile| profile.id == profile_id)
            .map(|profile| profile.channels)
            .unwrap_or_default())
    }

    /// Every provider stream.
    ///
    /// The list is paginated at up to 10 000 rows per page upstream and excludes streams
    /// whose M3U account is inactive, which is the same set the channel table shows.
    pub async fn streams(&self) -> Result<Vec<Stream>> {
        self.get_list("/channels/streams/", &[], "listing streams")
            .await
    }

    /// The streams attached to one channel, in playback order.
    ///
    /// `GET /channels/channels/{id}/streams/` orders by `channelstream__order`, so the array
    /// is the channel's own order rather than the stream table's.
    pub async fn channel_streams(&self, channel_id: i64) -> Result<Vec<Stream>> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Payload {
            Bare(Vec<Stream>),
            Paged { results: Vec<Stream> },
        }

        let payload: Payload = self
            .get_json(&format!("/channels/channels/{channel_id}/streams/"), &[])
            .await
            .with_context(|| format!("listing the streams of channel {channel_id}"))?;
        Ok(match payload {
            Payload::Bare(streams) => streams,
            // The view answers a bare array; a build that paginates it would answer this.
            Payload::Paged { results } => results,
        })
    }

    /// One channel's stream ids in playback order, from the channel's own row
    /// (`GET /channels/channels/{id}/`).
    ///
    /// Unlike [`Self::channel_streams`], which lists stream rows (through a stream serializer
    /// and whatever filters that view applies), this is exactly the list a stream-list write
    /// replaces. The row must carry `streams`: a row without it is an error, never an empty
    /// list.
    pub async fn channel_stream_ids(&self, channel_id: i64) -> Result<Vec<i64>> {
        #[derive(Deserialize)]
        struct Row {
            streams: Vec<i64>,
        }

        let row: Row = self
            .get_json(&format!("/channels/channels/{channel_id}/"), &[])
            .await
            .with_context(|| format!("reading channel {channel_id}"))?;
        Ok(row.streams)
    }

    /// Every M3U account.
    pub async fn m3u_accounts(&self) -> Result<Vec<M3uAccount>> {
        self.get_list("/m3u/accounts/", &[], "listing M3U accounts")
            .await
    }

    /// Every logo.
    ///
    /// The logo list is capped at 1 000 rows per page, which the shared list helper asks for,
    /// and it is ordered by name so a fixed limit never turns into a shifting window.
    pub async fn logos(&self) -> Result<Vec<Logo>> {
        self.get_list("/channels/logos/", &[], "listing logos")
            .await
    }

    /// Create a logo.
    ///
    /// `name` and `url` are the only writable columns. Not retried: `url` is unique, so a
    /// re-sent create answers 400 "A logo with this URL already exists" and would read as a
    /// failure for a create that worked. A failed create (that 400 after a lost answer, or
    /// any other error) is checked against the logos named like it (`?name=` is upstream's
    /// `icontains` filter): a logo with this exact URL is the one that landed, and comes back
    /// as the answer.
    pub async fn create_logo(&self, name: &str, url: &str) -> Result<Logo> {
        let body = serde_json::json!({ "name": name, "url": url });
        let created = self
            .write_json(
                &reqwest::Method::POST,
                "/channels/logos/",
                Some(&body),
                Retry::Once,
            )
            .await;
        match created {
            Ok(logo) => Ok(logo),
            Err(error) => {
                let landed: Option<Logo> = self
                    .get_list(
                        "/channels/logos/",
                        &[("name", name.to_string())],
                        "listing logos",
                    )
                    .await
                    .ok()
                    .and_then(|logos: Vec<Logo>| logos.into_iter().find(|logo| logo.url == url));
                landed.ok_or(error).context("creating a logo")
            }
        }
    }

    /// Create a channel group.
    ///
    /// Names are unique upstream and compared exactly (case-sensitive). The serializer's
    /// `create` is a get_or_create, but DRF's `UniqueValidator` on `name` runs first, so an
    /// existing name answers 400 "channel group with this name already exists" rather than
    /// the group: a retry after a lost answer would read as a failure for a create that
    /// worked. A failed create is therefore checked against the group list, and a group with
    /// this exact name comes back as the answer.
    pub async fn create_group(&self, name: &str) -> Result<ChannelGroup> {
        let body = serde_json::json!({ "name": name });
        let created = self
            .write_json(
                &reqwest::Method::POST,
                "/channels/groups/",
                Some(&body),
                Retry::Idempotent,
            )
            .await;
        match created {
            Ok(group) => Ok(group),
            Err(error) => {
                let landed = self
                    .groups()
                    .await
                    .ok()
                    .and_then(|groups| groups.into_iter().find(|group| group.name == name));
                landed
                    .ok_or(error)
                    .with_context(|| format!("creating the channel group {name:?}"))
            }
        }
    }

    /// Rename a channel group.
    ///
    /// A group that a provider still claims cannot be renamed in place: for a provider-only
    /// group upstream answers 400 "Cannot edit group with M3U account associations", and that
    /// body is what the error carries. A group that is claimed *and* already marked custom is
    /// renamed by creating a manual group, moving the manual channels onto it and demoting the
    /// provider rows, so the group that comes back can carry a different id than `group_id`
    /// (the demoted one is in the response's `demoted_group`, which this type does not read).
    ///
    /// Not retried: that create-and-move path is not idempotent, so a re-sent rename after a
    /// lost answer could split the group a second time.
    pub async fn rename_group(&self, group_id: i64, name: &str) -> Result<ChannelGroup> {
        let body = serde_json::json!({ "name": name });
        self.write_json(
            &reqwest::Method::PATCH,
            &format!("/channels/groups/{group_id}/"),
            Some(&body),
            Retry::Once,
        )
        .await
        .with_context(|| format!("renaming channel group {group_id} to {name:?}"))
    }

    /// Create a channel.
    ///
    /// Not retried: a re-sent create makes a second channel with the same name. Dispatcharr
    /// assigns the next free number when the payload names none, and creates the
    /// "Default Group" when it names no group.
    pub async fn create_channel(&self, channel: &ChannelCreate) -> Result<Channel> {
        let body = serde_json::to_value(channel).context("serializing the new channel")?;
        self.write_json(
            &reqwest::Method::POST,
            "/channels/channels/",
            Some(&body),
            Retry::Once,
        )
        .await
        .with_context(|| format!("creating the channel {:?}", channel.name))
    }

    /// Create channels from provider streams, asynchronously.
    ///
    /// Answers 202 with a Celery task id (plus a message, a stream count and a status):
    /// the channels appear some time later, and there is no task-status route for them, so
    /// the caller re-reads the channel list. Not retried, for the same reason as
    /// [`Self::create_channel`]
    pub async fn create_channels_from_streams(
        &self,
        request: &StreamsToChannels,
    ) -> Result<QueuedTask> {
        if request.stream_ids.is_empty() {
            bail!("creating channels from streams needs at least one stream id");
        }
        let body = serde_json::to_value(request).context("serializing the channel request")?;
        self.write_json(
            &reqwest::Method::POST,
            "/channels/channels/from-stream/bulk/",
            Some(&body),
            Retry::Once,
        )
        .await
        .with_context(|| {
            format!(
                "queueing channel creation for {} streams",
                request.stream_ids.len()
            )
        })
    }

    /// Apply a batch of channel edits in one request per chunk.
    ///
    /// `PATCH /channels/channels/edit/bulk/` validates every row through
    /// `ChannelSerializer(partial=True)` and applies them in one transaction, so a single
    /// invalid row rejects its whole chunk — the 400 body names the channel and the reason,
    /// which is what the error carries. Returns the number of channels the server reported
    /// updated (the length of the `channels` array it echoes back).
    ///
    /// Edits are chunked at `CHANNEL_EDIT_CHUNK_SIZE` and each chunk is retried like a read
    /// (5xx/429 with `Retry-After`, one JWT refresh on 401); re-applying an identical edit is
    /// a no-op, so a retried chunk cannot change a channel twice. A failing chunk names
    /// itself and how many channels were already confirmed.
    pub async fn edit_channels(&self, edits: &[ChannelEdit]) -> Result<usize> {
        if edits.is_empty() {
            // Upstream rejects an empty list with 400; nothing to do is a no-op.
            return Ok(0);
        }

        let total_chunks = edits.len().div_ceil(CHANNEL_EDIT_CHUNK_SIZE);
        let mut confirmed: usize = 0;
        let mut updated: usize = 0;
        for (index, chunk) in edits.chunks(CHANNEL_EDIT_CHUNK_SIZE).enumerate() {
            let chunk_no = index + 1;
            match self.patch_channel_edits(chunk).await {
                Ok(count) => {
                    confirmed += chunk.len();
                    updated += count;
                }
                Err(error) => {
                    anyhow::bail!(
                        "chunk {chunk_no}/{total_chunks} of the bulk channel edit failed after \
                         {confirmed} channels were confirmed: {error:#}"
                    );
                }
            }
        }
        Ok(updated)
    }

    /// [`Self::edit_channels`] with one answer per edit, in input order.
    ///
    /// A confirmed chunk answers `Some(Ok)` for each of its edits. A chunk rejected with 400
    /// (one invalid row rejects the whole transaction, so nothing of it landed) is retried
    /// one edit at a time, so the bad row fails alone instead of taking ~500 good ones down
    /// with it. Any other failure fails that chunk's edits and the walk goes on to the next
    /// chunk. `cancelled` is asked before every request: an edit never sent answers `None`.
    pub async fn edit_channels_each(
        &self,
        edits: &[ChannelEdit],
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Vec<Option<Result<()>>> {
        let mut results: Vec<Option<Result<()>>> = Vec::with_capacity(edits.len());
        for chunk in edits.chunks(CHANNEL_EDIT_CHUNK_SIZE) {
            if cancelled() {
                results.extend(chunk.iter().map(|_| None));
                continue;
            }
            match self.patch_channel_edits(chunk).await {
                Ok(_) => results.extend(chunk.iter().map(|_| Some(Ok(())))),
                Err(error) if is_rejected_row(&error) && chunk.len() > 1 => {
                    for edit in chunk {
                        if cancelled() {
                            results.push(None);
                            continue;
                        }
                        let result = self
                            .patch_channel_edits(std::slice::from_ref(edit))
                            .await
                            .map(|_| ())
                            .with_context(|| format!("editing channel {}", edit.id));
                        results.push(Some(result));
                    }
                }
                Err(error) => {
                    let message = format!("the bulk channel edit failed: {error:#}");
                    results.extend(
                        chunk
                            .iter()
                            .map(|_| Some(Err(anyhow::anyhow!("{message}")))),
                    );
                }
            }
        }
        results
    }

    /// PATCH one chunk of channel edits.
    async fn patch_channel_edits(&self, chunk: &[ChannelEdit]) -> Result<usize> {
        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            channels: Vec<Channel>,
        }

        let body = serde_json::to_value(chunk).context("serializing the channel edits")?;
        let response: Response = self
            .write_json(
                &reqwest::Method::PATCH,
                "/channels/channels/edit/bulk/",
                Some(&body),
                Retry::Idempotent,
            )
            .await?;
        Ok(response.channels.len())
    }

    /// Rename channels with a server-side regex find/replace.
    ///
    /// `POST /channels/channels/edit/bulk-regex/` rewrites `name` for every channel it is
    /// given and returns `{"success": true, "updated_count": n}`; names the pattern does not
    /// match, or that would come out empty, are left alone and not counted. A request whose
    /// ids match no channel answers 404, which the error reports.
    ///
    /// Not retried: applying the pattern twice is a different edit from applying it once
    /// (`HD` → `HDTV` → `HDTVTV`), so a lost response must surface rather than repeat.
    pub async fn rename_channels_regex(&self, rename: &ChannelRegexRename) -> Result<usize> {
        if rename.channel_ids.is_empty() {
            bail!("a regex rename needs at least one channel id");
        }
        if rename.find.trim().is_empty() {
            bail!("a regex rename needs a `find` pattern");
        }

        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            updated_count: i64,
        }

        let body = serde_json::to_value(rename).context("serializing the regex rename")?;
        let response: Response = self
            .write_json(
                &reqwest::Method::POST,
                "/channels/channels/edit/bulk-regex/",
                Some(&body),
                Retry::Once,
            )
            .await
            .with_context(|| {
                format!(
                    "renaming {} channels with the pattern {:?}",
                    rename.channel_ids.len(),
                    rename.find
                )
            })?;
        Ok(response.updated_count.max(0) as usize)
    }

    /// Enable or disable channels in one channel profile.
    ///
    /// `PATCH /channels/profiles/{id}/channels/bulk-update/` takes
    /// `{"channels": [{"channel_id", "enabled"}]}`, creates the membership rows that are
    /// missing, and answers `{status, updated, created, invalid_channels}`. Returns the
    /// number of memberships written (updated plus created). A channel id that does not exist
    /// makes the whole request a 400, which the error reports.
    ///
    /// Chunked at `MEMBERSHIP_CHUNK_SIZE` and retried like a read; setting the same
    /// membership twice changes nothing.
    pub async fn set_profile_membership(
        &self,
        profile_id: i64,
        channels: &[(i64, bool)],
    ) -> Result<usize> {
        if channels.is_empty() {
            return Ok(0);
        }

        let total_chunks = channels.len().div_ceil(MEMBERSHIP_CHUNK_SIZE);
        let mut confirmed: usize = 0;
        let mut written: usize = 0;
        for (index, chunk) in channels.chunks(MEMBERSHIP_CHUNK_SIZE).enumerate() {
            let chunk_no = index + 1;
            match self.patch_membership_chunk(profile_id, chunk).await {
                Ok(count) => {
                    confirmed += chunk.len();
                    written += count;
                }
                Err(error) => {
                    anyhow::bail!(
                        "chunk {chunk_no}/{total_chunks} of the profile {profile_id} membership \
                         update failed after {confirmed} channels were confirmed: {error:#}"
                    );
                }
            }
        }
        Ok(written)
    }

    /// [`Self::set_profile_membership`] with one answer per channel, in input order, the way
    /// [`Self::edit_channels_each`] answers: a 400 chunk (an unknown channel id rejects the
    /// whole request) is retried one channel at a time, any other failure fails its chunk
    /// only, and a channel never sent because `cancelled` answered true is `None`.
    pub async fn set_profile_membership_each(
        &self,
        profile_id: i64,
        channels: &[(i64, bool)],
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Vec<Option<Result<()>>> {
        let mut results: Vec<Option<Result<()>>> = Vec::with_capacity(channels.len());
        for chunk in channels.chunks(MEMBERSHIP_CHUNK_SIZE) {
            if cancelled() {
                results.extend(chunk.iter().map(|_| None));
                continue;
            }
            match self.patch_membership_chunk(profile_id, chunk).await {
                Ok(_) => results.extend(chunk.iter().map(|_| Some(Ok(())))),
                Err(error) if is_rejected_row(&error) && chunk.len() > 1 => {
                    for membership in chunk {
                        if cancelled() {
                            results.push(None);
                            continue;
                        }
                        let result = self
                            .patch_membership_chunk(profile_id, std::slice::from_ref(membership))
                            .await
                            .map(|_| ())
                            .with_context(|| {
                                format!("updating channel {} in profile {profile_id}", membership.0)
                            });
                        results.push(Some(result));
                    }
                }
                Err(error) => {
                    let message =
                        format!("the profile {profile_id} membership update failed: {error:#}");
                    results.extend(
                        chunk
                            .iter()
                            .map(|_| Some(Err(anyhow::anyhow!("{message}")))),
                    );
                }
            }
        }
        results
    }

    /// PATCH one chunk of profile memberships.
    async fn patch_membership_chunk(
        &self,
        profile_id: i64,
        chunk: &[(i64, bool)],
    ) -> Result<usize> {
        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            updated: i64,
            #[serde(default)]
            created: i64,
            #[serde(default)]
            invalid_channels: Vec<i64>,
        }

        let body = serde_json::json!({
            "channels": chunk
                .iter()
                .map(|(channel_id, enabled)| serde_json::json!({
                    "channel_id": channel_id,
                    "enabled": enabled,
                }))
                .collect::<Vec<_>>(),
        });
        let response: Response = self
            .write_json(
                &reqwest::Method::PATCH,
                &format!("/channels/profiles/{profile_id}/channels/bulk-update/"),
                Some(&body),
                Retry::Idempotent,
            )
            .await?;
        if !response.invalid_channels.is_empty() {
            // The view answers 400 for this, so reaching here means a build answered 200
            // with the same information; either way the caller must not read it as success.
            bail!(
                "the profile update rejected channel ids {:?}",
                response.invalid_channels
            );
        }
        Ok((response.updated + response.created).max(0) as usize)
    }

    /// Set a channel's ordered stream list.
    ///
    /// `PATCH /channels/channels/{id}/` with `streams`: the array order becomes the playback
    /// order (`ChannelSerializer.update` writes `ChannelStream.order` from the index), and
    /// streams missing from the array are unlinked. This is the one-channel form;
    /// [`Self::bulk_set_channel_streams`] carries many channels per request through
    /// `edit/bulk`, and falls back to this route to attribute a rejected chunk per channel.
    ///
    /// Retried: re-applying the same list rewrites the same orders.
    pub async fn set_channel_streams(
        &self,
        channel_id: i64,
        stream_ids: &[i64],
    ) -> Result<Channel> {
        let body = serde_json::json!({ "streams": stream_ids });
        self.write_json(
            &reqwest::Method::PATCH,
            &format!("/channels/channels/{channel_id}/"),
            Some(&body),
            Retry::Idempotent,
        )
        .await
        .with_context(|| {
            format!(
                "setting {} streams on channel {channel_id}",
                stream_ids.len()
            )
        })
    }

    /// Set many channels' ordered stream lists through the bulk edit.
    ///
    /// `PATCH /channels/channels/edit/bulk/` with one `{id, streams}` row per channel
    /// (`edit_bulk` in `apps/channels/api_views.py`): the `streams` array REPLACES the
    /// channel's whole list and its INDEX is the playback order (`order = enumerate(ids)`),
    /// streams missing from the array are unlinked, and the chunk applies in a single
    /// transaction. A streams-only row carries no other column, so it skips the column update
    /// and only touches the `ChannelStream` rows.
    ///
    /// One invalid row rejects its WHOLE chunk before anything is written — a bad stream id
    /// fails the row's `PrimaryKeyRelatedField` validation, an unknown channel id is
    /// reported, and `edit_bulk` answers 400 with the per-row errors — so a rejected chunk is
    /// retried row by row through [`Self::set_channel_streams`] and the failures are
    /// attributed per channel instead of failing ~500 channels over one bad id. A chunk is
    /// also rejected whole when two rows name the same channel, or when one list names the
    /// same stream twice (the `unique_channel_stream` constraint); the per-channel retry
    /// attributes those too.
    ///
    /// Chunked at `CHANNEL_EDIT_CHUNK_SIZE` with the same idempotent retry as
    /// [`Self::edit_channels`]: re-applying the same lists rewrites the same orders, so a
    /// retried chunk is safe. The answer is in input order, one entry per channel; an `Err`
    /// is that channel's failure and does not stop the others.
    pub async fn bulk_set_channel_streams(
        &self,
        lists: &[(i64, Vec<i64>)],
    ) -> Vec<(i64, Result<()>)> {
        let mut results = Vec::with_capacity(lists.len());
        for chunk in lists.chunks(CHANNEL_EDIT_CHUNK_SIZE) {
            let edits: Vec<ChannelEdit> = chunk
                .iter()
                .map(|(channel_id, stream_ids)| {
                    let mut edit = ChannelEdit::new(*channel_id);
                    edit.streams = Some(stream_ids.clone());
                    edit
                })
                .collect();
            match self.patch_channel_edits(&edits).await {
                Ok(_) => results.extend(chunk.iter().map(|(id, _)| (*id, Ok(())))),
                Err(error) if !is_rejected_row(&error) => {
                    // Not a rejected row (a 5xx after retries, a timeout, a lost answer):
                    // 500 single PATCHes would only repeat the outage. The chunk fails.
                    let message = format!("the bulk stream-list write failed: {error:#}");
                    results.extend(
                        chunk
                            .iter()
                            .map(|(id, _)| (*id, Err(anyhow::anyhow!("{message}")))),
                    );
                }
                Err(_) => {
                    // The chunk was rejected whole (400); attribute it per channel.
                    for (channel_id, stream_ids) in chunk {
                        let result = self
                            .set_channel_streams(*channel_id, stream_ids)
                            .await
                            .map(|_| ());
                        results.push((*channel_id, result));
                    }
                }
            }
        }
        results
    }

    /// Write one stream's probe output.
    ///
    /// `PATCH /channels/streams/{id}/` with `stream_stats`: `StreamSerializer` keeps
    /// `stream_stats` writable even for a provider stream (it makes the descriptive columns
    /// read-only, not this one), so a probe can write what it measured without touching
    /// anything else about the stream.
    ///
    /// `stream_stats_updated_at` is sent with it. Nothing upstream stamps it for an API write
    /// — only the live proxy does, on a play — and Dispatcharr's own stats delta
    /// (`/channels/<id>/streams/stats/?since=…`) filters on it, so leaving it stale would hide
    /// the write from every reader of that column. One clock, the caller's.
    ///
    /// Retried like a read: re-applying the same stats changes nothing a reader can see
    /// beyond the timestamp.
    pub async fn update_stream_stats(
        &self,
        stream_id: i64,
        stats: &serde_json::Value,
    ) -> Result<Stream> {
        let body = serde_json::json!({
            "stream_stats": stats,
            "stream_stats_updated_at": chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        });
        self.write_json(
            &reqwest::Method::PATCH,
            &format!("/channels/streams/{stream_id}/"),
            Some(&body),
            Retry::Idempotent,
        )
        .await
        .with_context(|| format!("writing the stats of stream {stream_id}"))
    }

    /// Assign or clear EPG entries on channels.
    ///
    /// Dispatcharr queues the programme parse for every changed entry itself, which is the
    /// step the plugin had to trigger by hand through a Django signal. `channels_updated` can
    /// legitimately be smaller than the number of associations: a channel already on that
    /// entry is skipped upstream, and that is not an error.
    ///
    /// `epg_data_id` is `Option` because the endpoint accepts `null` and that is how a
    /// channel's guide is cleared (`{"channel_id": 1, "epg_data_id": null}`); the view
    /// assigns it straight to the column, and a null queues no programme refresh because
    /// upstream only collects the truthy ids.
    ///
    /// A retry after a timeout can re-apply a chunk the server already committed (the
    /// response was lost, not the write). The endpoint is idempotent per
    /// (channel, entry) pair — re-applying an assignment changes nothing — so a retried
    /// chunk is safe; note only that `channels_updated` counts channels the confirmed write
    /// actually changed, and may under-report when a retry re-applied an already-committed
    /// chunk.
    ///
    /// Associations are sent in chunks of at most `BATCH_SET_EPG_CHUNK_SIZE`. Each chunk
    /// gets the same retry/refresh treatment as the GET path (retry 5xx/429 honouring
    /// `Retry-After` with exponential backoff, one JWT refresh on 401), and the returned
    /// counts are summed. A chunk failure names the chunk and how many associations were
    /// already confirmed, so the caller can report partial progress.
    pub async fn batch_set_epg(&self, associations: &[(i64, Option<i64>)]) -> Result<usize> {
        let outcome = self.batch_set_epg_outcome(associations).await;
        match outcome.failure {
            Some(failure) => Err(anyhow::anyhow!(failure)),
            None => Ok(outcome.updated),
        }
    }

    /// [`batch_set_epg`](Self::batch_set_epg) with the per-chunk truth kept: how many
    /// leading associations landed before a chunk failed, so a caller recording one row
    /// per association marks the landed rows `ok` and only the rest failed (phase 3
    /// review: a failed later chunk used to fail the whole batch's rows, and undo then
    /// skipped writes that had in fact landed). Chunks are posted in order and the walk
    /// stops at the first failure, so `confirmed` is a prefix length of `associations`.
    pub async fn batch_set_epg_outcome(
        &self,
        associations: &[(i64, Option<i64>)],
    ) -> BatchEpgOutcome {
        let mut outcome = BatchEpgOutcome::default();
        if associations.is_empty() {
            // Upstream rejects an empty list with 400; a run with nothing to write is a
            // no-op, not a failure.
            return outcome;
        }

        let total_chunks = associations.len().div_ceil(BATCH_SET_EPG_CHUNK_SIZE);
        for (index, chunk) in associations.chunks(BATCH_SET_EPG_CHUNK_SIZE).enumerate() {
            let chunk_no = index + 1;
            match self.post_epg_chunk(chunk).await {
                Ok(count) => {
                    outcome.confirmed += chunk.len();
                    outcome.updated += count;
                }
                Err(error) => {
                    let confirmed = outcome.confirmed;
                    outcome.failure = Some(format!(
                        "chunk {chunk_no}/{total_chunks} failed after {confirmed} associations were confirmed: {error:#}"
                    ));
                    return outcome;
                }
            }
        }
        outcome
    }

    /// POST one chunk of EPG associations.
    async fn post_epg_chunk(&self, chunk: &[(i64, Option<i64>)]) -> Result<usize> {
        #[derive(Serialize)]
        struct Association {
            channel_id: i64,
            epg_data_id: Option<i64>,
        }
        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            channels_updated: i64,
            #[serde(default)]
            success: bool,
        }

        let body = serde_json::json!({
            "associations": chunk
                .iter()
                .map(|(channel_id, epg_data_id)| Association {
                    channel_id: *channel_id,
                    epg_data_id: *epg_data_id,
                })
                .collect::<Vec<_>>(),
        });

        let response: Response = self
            .write_json(
                &reqwest::Method::POST,
                "/channels/channels/batch-set-epg/",
                Some(&body),
                Retry::Idempotent,
            )
            .await?;
        if !response.success {
            bail!("POST /channels/channels/batch-set-epg/ reported failure upstream");
        }
        Ok(response.channels_updated.max(0) as usize)
    }
}
