package com.nuvio.app.features.streams

import co.touchlab.kermit.Logger
import com.nuvio.app.core.build.AppFeaturePolicy
import com.nuvio.app.features.addons.AddonRepository
import com.nuvio.app.features.addons.buildAddonResourceUrl
import com.nuvio.app.features.addons.enabledAddons
import com.nuvio.app.features.addons.httpGetText
import com.nuvio.app.features.cloudstream.CloudStreamRepository
import com.nuvio.app.features.debrid.DebridSettingsRepository
import com.nuvio.app.features.debrid.DirectDebridPlayableResult
import com.nuvio.app.features.debrid.DirectDebridPlaybackResolver
import com.nuvio.app.features.details.MetaDetails
import com.nuvio.app.features.details.MetaDetailsRepository
import com.nuvio.app.features.details.MetaVideo
import com.nuvio.app.features.downloads.DownloadEnqueueResult
import com.nuvio.app.features.downloads.DownloadsRepository
import com.nuvio.app.features.player.PlayerSettingsRepository
import com.nuvio.app.features.plugins.PluginRepository
import com.nuvio.app.features.plugins.PluginsUiState
import com.nuvio.app.features.plugins.isExcludedByPluginQualityFilter
import com.nuvio.app.features.plugins.pluginContentId
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull

object BatchEpisodeDownloads {
    private val log = Logger.withTag("BatchDownloads")

    suspend fun enqueueEpisodes(
        meta: MetaDetails,
        episodes: List<MetaVideo>,
        preferredSource: StreamItem? = null,
        onEpisodeFinished: (BatchEpisodeDownloadProgress) -> Unit = {},
    ): BatchEpisodeDownloadSummary {
        DownloadsRepository.ensureLoaded()
        PlayerSettingsRepository.ensureLoaded()
        val playerSettings = PlayerSettingsRepository.uiState.value
        val debridSettings = DebridSettingsRepository.snapshot()
        val installedAddonNames = AddonRepository.uiState.value.addons
            .enabledAddons()
            .map { addon -> addon.displayTitle.ifBlank { addon.manifest?.name.orEmpty() } }
            .filter { it.isNotBlank() }
            .toSet()

        var queued = 0
        var replaced = 0
        var skipped = 0
        var failed = 0

        episodes.forEachIndexed { index, episode ->
            val stream = resolveDownloadStream(
                meta = meta,
                episode = episode,
                installedAddonNames = installedAddonNames,
                preferredSource = preferredSource,
            )

            if (stream == null) {
                failed += 1
                onEpisodeFinished(BatchEpisodeDownloadProgress(index + 1, episodes.size, episode))
                return@forEachIndexed
            }

            val result = DownloadsRepository.enqueueFromStream(
                contentType = meta.type,
                videoId = episode.id.takeIf { it.isNotBlank() } ?: buildBatchPlaybackVideoId(meta.id, episode),
                parentMetaId = meta.id,
                parentMetaType = meta.type,
                title = meta.name,
                logo = meta.logo,
                poster = meta.poster,
                background = meta.background,
                seasonNumber = episode.season,
                episodeNumber = episode.episode,
                episodeTitle = episode.title,
                episodeThumbnail = episode.thumbnail,
                episodeOverview = episode.overview,
                stream = stream,
            )

            when (result) {
                DownloadEnqueueResult.Started -> queued += 1
                DownloadEnqueueResult.Replaced -> replaced += 1
                DownloadEnqueueResult.MissingUrl,
                DownloadEnqueueResult.UnsupportedFormat -> skipped += 1
            }
            onEpisodeFinished(BatchEpisodeDownloadProgress(index + 1, episodes.size, episode))
        }

        return BatchEpisodeDownloadSummary(
            requested = episodes.size,
            queued = queued,
            replaced = replaced,
            skipped = skipped,
            failed = failed,
        )
    }

    private suspend fun resolveDownloadStream(
        meta: MetaDetails,
        episode: MetaVideo,
        installedAddonNames: Set<String>,
        preferredSource: StreamItem?,
    ): StreamItem? {
        val streams = loadStreams(
            meta = meta,
            episode = episode,
            preferredSource = preferredSource,
        )
        if (streams.isEmpty()) return null

        PlayerSettingsRepository.ensureLoaded()
        val playerSettings = PlayerSettingsRepository.uiState.value
        val debridSettings = DebridSettingsRepository.snapshot()
        val mode = playerSettings.streamAutoPlayMode.takeUnless { it == StreamAutoPlayMode.MANUAL }
            ?: StreamAutoPlayMode.FIRST_STREAM

        val selected = if (preferredSource != null) {
            streams.firstOrNull { it.matchesBatchSource(preferredSource) }
        } else {
            StreamAutoPlaySelector.selectAutoPlayStream(
                streams = streams,
                mode = mode,
                regexPattern = playerSettings.streamAutoPlayRegex,
                source = playerSettings.streamAutoPlaySource,
                installedAddonNames = installedAddonNames,
                selectedAddons = playerSettings.streamAutoPlaySelectedAddons,
                selectedPlugins = playerSettings.streamAutoPlaySelectedPlugins,
                debridEnabled = debridSettings.canResolvePlayableLinks,
                activeResolverProviderId = debridSettings.activeResolverProviderId,
            ) ?: streams.firstOrNull { it.playableDirectUrl != null }
        }

        return when {
            selected == null -> null
            selected.playableDirectUrl != null -> selected
            DirectDebridPlaybackResolver.shouldResolveToPlayableStream(selected) -> {
                when (val resolved = DirectDebridPlaybackResolver.resolveToPlayableStream(selected, episode.season, episode.episode)) {
                    is DirectDebridPlayableResult.Success -> resolved.stream.takeIf { it.playableDirectUrl != null }
                    else -> null
                }
            }
            else -> null
        }
    }

    private suspend fun loadStreams(
        meta: MetaDetails,
        episode: MetaVideo,
        preferredSource: StreamItem? = null,
    ): List<StreamItem> {
        MetaDetailsRepository.findEmbeddedStreams(episode.id)
            .takeIf { it.isNotEmpty() }
            ?.let { embeddedStreams ->
                if (preferredSource == null) return embeddedStreams
                val matchingEmbeddedStreams = embeddedStreams.filter { it.matchesBatchSource(preferredSource) }
                if (matchingEmbeddedStreams.isNotEmpty()) return matchingEmbeddedStreams
            }

        val type = meta.type
        val videoId = episode.id.takeIf { it.isNotBlank() } ?: buildBatchPlaybackVideoId(meta.id, episode)
        val cloudStreamRequest = buildCloudStreamSearchRequest(
            type = type,
            videoId = videoId,
            parentMetaId = meta.id,
            parentMetaType = meta.type,
            season = episode.season,
            episode = episode.episode,
            searchTitle = episode.title.takeIf { it.isNotBlank() } ?: meta.name,
        )

        return coroutineScope {
            val pluginUiState = if (AppFeaturePolicy.pluginsEnabled) {
                PluginRepository.initialize()
                PluginRepository.uiState.value
            } else {
                PluginsUiState(pluginsEnabled = false)
            }
            val installedAddons = AddonRepository.uiState.value.addons.enabledAddons()
            val addonTargets = installedAddons.mapNotNull { addon ->
                val manifest = addon.manifest ?: return@mapNotNull null
                val supportsStream = manifest.resources.any { resource ->
                    resource.name == "stream" &&
                        resource.types.contains(type) &&
                        (resource.idPrefixes.isEmpty() || resource.idPrefixes.any { prefix -> videoId.startsWith(prefix) })
                }
                if (!supportsStream) return@mapNotNull null
                InstalledStreamAddonTarget(
                    addonName = addon.displayTitle.ifBlank { manifest.name },
                    addonId = addon.streamAddonInstanceId(manifest.id),
                    manifest = manifest,
                )
            }

            val pluginGroups = if (AppFeaturePolicy.pluginsEnabled) {
                PluginRepository.getEnabledScrapersForType(type).toPluginProviderGroups(
                    repositories = pluginUiState.repositories,
                    groupByRepository = pluginUiState.groupStreamsByRepository,
                )
            } else {
                emptyList()
            }
            val cloudGroups = if (AppFeaturePolicy.pluginsEnabled) {
                cloudStreamProviderGroupsForRequest(type, cloudStreamRequest)
            } else {
                emptyList()
            }
            val cloudSemaphore = Semaphore(6)

            val addonJobs = addonTargets.map { target ->
                async {
                    val url = buildAddonResourceUrl(
                        manifestUrl = target.manifest.transportUrl,
                        resource = "stream",
                        type = type,
                        id = videoId,
                    )
                    runCatchingUnlessCancelled {
                        val payload = withTimeoutOrNull(BATCH_PROVIDER_TIMEOUT_MS) {
                            httpGetText(url)
                        } ?: error("${target.addonName} timed out")
                        StreamParser.parse(
                            payload = payload,
                            addonName = target.addonName,
                            addonId = target.addonId,
                            addonLogo = target.manifest.logoUrl,
                        )
                    }.getOrElse { error ->
                        log.d(error) { "Batch add-on stream fetch failed provider=${target.addonName}" }
                        emptyList()
                    }
                }
            }

            val pluginJobs = pluginGroups.flatMap { group ->
                group.scrapers.map { scraper ->
                    async {
                        val result = withTimeoutOrNull(BATCH_PROVIDER_TIMEOUT_MS) {
                            PluginRepository.executeScraper(
                                scraper = scraper,
                                tmdbId = pluginContentId(
                                    videoId = videoId,
                                    season = episode.season,
                                    episode = episode.episode,
                                ),
                                mediaType = type,
                                season = episode.season,
                                episode = episode.episode,
                            )
                        } ?: Result.failure(Throwable("${scraper.name} timed out"))
                        result.getOrDefault(emptyList())
                            .filterNot { it.isExcludedByPluginQualityFilter(pluginUiState.excludedQualities) }
                            .map { item ->
                                item.toStreamItem(
                                    scraper = scraper,
                                    addonName = group.addonName,
                                    addonId = group.addonId,
                                )
                            }
                    }
                }
            }

            val cloudJobs = cloudGroups.map { group ->
                async {
                    cloudSemaphore.withPermit {
                        withTimeoutOrNull(BATCH_CLOUDSTREAM_TIMEOUT_MS) {
                            resolveCloudStreamProviderStreams(group, cloudStreamRequest).streams
                        }.orEmpty()
                    }
                }
            }

            (addonJobs + pluginJobs + cloudJobs)
                .awaitAll()
                .flatten()
                .distinctBy { stream ->
                    listOf(
                        stream.addonId,
                        stream.sourceName.orEmpty(),
                        stream.streamLabel,
                        stream.playableDirectUrl.orEmpty(),
                        stream.url.orEmpty(),
                    )
                }
        }
    }
}

data class BatchEpisodeDownloadProgress(
    val completed: Int,
    val total: Int,
    val episode: MetaVideo,
)

data class BatchEpisodeDownloadSummary(
    val requested: Int,
    val queued: Int,
    val replaced: Int,
    val skipped: Int,
    val failed: Int,
) {
    val successful: Int
        get() = queued + replaced
}

private fun StreamItem.matchesBatchSource(preferred: StreamItem): Boolean {
    if (addonId != preferred.addonId) return false
    if (!sourceName.equals(preferred.sourceName, ignoreCase = true)) return false

    return preferred.streamType == null || streamType.equals(preferred.streamType, ignoreCase = true)
}

private fun buildBatchPlaybackVideoId(parentMetaId: String, episode: MetaVideo): String =
    listOfNotNull(
        parentMetaId,
        episode.season?.let { "s$it" },
        episode.episode?.let { "e$it" },
    ).joinToString(":")

private const val BATCH_PROVIDER_TIMEOUT_MS = 30_000L
private const val BATCH_CLOUDSTREAM_TIMEOUT_MS = 120_000L
