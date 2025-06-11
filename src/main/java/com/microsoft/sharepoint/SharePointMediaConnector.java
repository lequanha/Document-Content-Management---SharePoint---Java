package com.microsoft.sharepoint;

import com.acl.AclInheritanceType;
import com.error.BadRequestType;
import com.file.*;
import com.media.MediaChangeLogDto;
import com.media.MediaType;
import com.media.SharePointConnectionParametersDto;
import com.messages.DirListingPayload;
import com.messages.ScanErrorDto;
import com.messages.ScanTaskParameters;
import com.exceptions.MediaConnectionException;
import com.FileContentParams;
import com.StreamMediaItemsParams;
import com.microsoft.*;
import com.progress.ProgressTracker;
import com.utils.FileTypeUtils;
import com.utils.Pair;
import com.utils.TimeSource;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.middleware.share.*;
import com.middleware.share.queryoptions.*;
import com.rometools.utils.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.*;
import java.util.List;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Created by uri on 07/08/2016.
 * <p>
 * TODO - support browsing by folders and not just lists
 * https://docauthority.sharepoint.com/tests/_api/web/GetFolderByServerRelativeUrl('/tests/Missoula%20SubZone/CitySub')?$expand=Folders,ListItemAllFields&$select*
 * We still need the list item ID (and we can save it as today), but we can browse the folders directly.
 * We can create a stream of files and folders (as today - each item is either a file or a folder)
 * that comes from the alternative API.
 * The main added value is that we have something that is more resilient.
 * The main disadvantage is that we still must have the other API for file attachments
 */
public class SharePointMediaConnector extends MicrosoftConnectorBase {

//    public static final String LIBRARY_PREFIX = "/library:";

    private static final Logger logger = LoggerFactory.getLogger(SharePointMediaConnector.class);

    // SP internal folders
    private static final List<String> EXCLUDED_LIBS = Arrays.asList("/odata__catalogs", "/odata__catalogslt", "/lists",
            "/iwconvertedforms", "/formservertemplates", "/sitepages", "/m");
    private static final String EXCLUDED_LIB_PREFIX = "odata__";

    private String host;
    private String port;
    private String scheme;

    private boolean sharePointOnline;

    private String baseUri;
    private String baseUriWithBasePath;
    private int siteCrawlMaxDepth;
    private String[] charsToFilter;

    private static Map<String, SharePointMediaConnector> fstLvlFoldersToConnectorMap = Maps.newConcurrentMap();
    private static final Map<String, SharePointMediaConnector> basePathToConnectorMap = Maps.newConcurrentMap();

    private TimeSource timeSource = TimeSource.create();

    protected SharePointMediaConnector(SharePointConnectionParametersDto sharePointConnectionDetailsDto,
                                       MSAppInfo appInfo,
                                       int maxRetries,
                                       int pageSize,
                                       long maxSupportFileSize,
                                       MSConnectionConfig connectionConfig,
                                       int siteCrawlMaxDepth,
                                       String folderToFail,
                                       List<String> foldersToFilter,
                                       boolean isSpecialCharsSupported,
                                       int maxPathCrawlingDepth,
                                       int maxIdenticalNameInPath,
                                       boolean pathMismatchSkip,
                                       String... charsToFilter) {

        super(sharePointConnectionDetailsDto.getUsername(),
                sharePointConnectionDetailsDto.getPassword(),
                appInfo,
                pageSize,
                maxRetries,
                maxSupportFileSize,
                connectionConfig,
                folderToFail,
                isSpecialCharsSupported,
                maxPathCrawlingDepth,
                maxIdenticalNameInPath,
                pathMismatchSkip,
                foldersToFilter);

        String domain = StringUtils.isBlank(sharePointConnectionDetailsDto.getDomain()) ? null : sharePointConnectionDetailsDto.getDomain();
        String url = sharePointConnectionDetailsDto.getUrl();
        sharePointOnline = sharePointConnectionDetailsDto.getSharePointOnline() != null ?
                sharePointConnectionDetailsDto.getSharePointOnline() : false;
        extractParametersFromUrl(url);

        this.siteCrawlMaxDepth = siteCrawlMaxDepth;
        this.charsToFilter = charsToFilter;
        init(domain, null, url, appInfo, charsToFilter);

        baseUri = scheme + "://" + host;
        if (port != null) {
            baseUri += ":" + port;
        }
        baseUriWithBasePath = baseUri + basePath;
    }

    @Override
    public void streamMediaItems(StreamMediaItemsParams params) {

        closedResourceStaleConnections(); // TODO Oren - needed?
        String path = params.getScanParams().getPath();
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(path);
        String basePathAddendum = resolveRootFolderBasePath(key, false);
        if (!StringUtils.EMPTY.equals(basePathAddendum)) {
            Consumer<ClaFilePropertiesDto> filePropConsumerWrapper = getFilePropertiesConsumerForAddendumBasePath(params.getFilePropertiesConsumer(), basePathAddendum);
            params.setFilePropertiesConsumer(filePropConsumerWrapper);
            getOrCreateCachedConnectorForRootSite(basePathAddendum)
                    .streamMediaItems(params);
            postScan();
            return;
        }

        Long runId = params.getScanParams().getRunId();
        String subSite = key.getSite();
        String library = getLibraryName(key.getPath(), subSite);
        if (subSite != null) {
            String strippedPath = key.getPath();
            int libInx = strippedPath.toLowerCase().indexOf(library.toLowerCase());
            if (libInx < 0) {
                logger.debug("Could not locate library {} in path {}, subSite {}", library, path, subSite);     // TODO - Oren?
            } else {
                library = strippedPath.substring(libInx);
            }
        }
        logger.info("Streaming media items for path {} (library {}, sub-site {})", path, library, subSite);
        try {
            logger.debug("Stream files from library {} (path={})", library, path);
            SharePointExtendedFolder list = getFolderProperties(subSite, library);
            String normalizedPath = SharePointParseUtils.normalizePath(params.getScanParams().getPath());
            //Get the base permissions on the list
            if (list.getListId() == null) {
                StreamSiteData siteStreamData = new StreamSiteData(params, normalizedPath, subSite, 0);
                streamFileScanDetailsFromSite(siteStreamData);
            } else {
                logger.debug("Stream fileScan details from list {}", list);
                streamFileScanDetailsFromList(
                        new StreamListData(normalizedPath, list.getListId(), library, subSite, params));
            }
        } catch (FileNotFoundException e) {
            logger.warn("Library not found while listing items in SharePoint {}. {}", path, e);
            ClaFilePropertiesDto propsDto = ClaFilePropertiesDto.create().addError(
                    createScanError("Library not found while listing items in sharepoint",
                            e, path, runId));
            params.getFilePropertiesConsumer().accept(propsDto);
        }

        closedResourceStaleConnections();
    }

    private Consumer<ClaFilePropertiesDto> getFilePropertiesConsumerForAddendumBasePath(Consumer<ClaFilePropertiesDto> filePropertiesConsumer, String basePathAddendum) {
        return prop -> {
            Optional.ofNullable(prop.getMediaItemId())
                    .map(id -> SharePointParseUtils.applyBasePathCompletionToMediaItemId(basePathAddendum, id))
                    .ifPresent(prop::setMediaItemId);
            filePropertiesConsumer.accept(prop);
        };
    }

    private SharePointMediaConnector recreateConnectorWithAdjustedParams(String basePathAddendum) {
        logger.trace("Recreating sharepoint-connector with addendum: {}", basePathAddendum);
        SharePointConnectionParametersDto dto = new SharePointConnectionParametersDto();
        dto.setUsername(userName);
        dto.setPassword(password);
        dto.setDomain(domain);
        dto.setUrl(SharePointParseUtils.normalizePath(url + "/" + basePathAddendum));
        return new SharePointMediaConnector(dto,
                appInfo,
                maxRetries,
                pageSize,
                maxFileSize,
                connectionConfig,
                siteCrawlMaxDepth,
                folderToFail,
                foldersToFilter,
                isSpecialCharsSupported,
                maxPathCrawlingDepth,
                maxIdenticalNameInPath,
                pathMismatchSkip,
                charsToFilter);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void concurrentStreamMediaItems(StreamMediaItemsParams params) {

        closedResourceStaleConnections();

        Long runId = params.getScanParams().getRunId();
        String path = params.getScanParams().getPath();
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(path);
        String basePathAddendum = resolveRootFolderBasePath(key, false);
        if (!StringUtils.EMPTY.equals(basePathAddendum)) {
            Consumer<ClaFilePropertiesDto> filePropConsumerWrapper = getFilePropertiesConsumerForAddendumBasePath(params.getFilePropertiesConsumer(), basePathAddendum);
            params.setFilePropertiesConsumer(filePropConsumerWrapper);
            getOrCreateCachedConnectorForRootSite(basePathAddendum)
                    .concurrentStreamMediaItems(params);
            postScan();
            return;
        }
        String subSite = key.getSite();
        String library = getLibraryName(key.getPath(), subSite);
        if (subSite != null) {
            String strippedPath = key.getPath();
            int libInx = strippedPath.toLowerCase().indexOf(library.toLowerCase());
            if (libInx < 0) {
                logger.debug("Could not locate library {} in path {}, subSite {}", library, path, subSite);
            } else {
                library = strippedPath.substring(libInx);
            }
        }
        try {
            logger.debug("Stream files from library {} (path={}, sub-site={})", library, path, subSite);
            SharePointExtendedFolder list = getFolderProperties(subSite, library);
            //Get the base permissions on the list
            if (list.getListId() == null) {
                StreamSiteData siteData = new StreamSiteData(params, params.getScanParams().getPath(), subSite, 0);

                streamFileScanDetailsFromSite(new ConcurrentStreamData(params.getForkJoinPool(), siteData, params.getDirectoryListingConsumer()));
            } else {
                logger.debug("Stream fileScan details from list {}", list);
                StreamListData streamListData = new StreamListData(params.getScanParams().getPath(),
                        list.getListId(), library, subSite, params);
                streamFileScanDetailsFromList(
                        new ConcurrentStreamData(params.getForkJoinPool(), streamListData, params.getDirectoryListingConsumer()));
            }
        } catch (FileNotFoundException e) {
            logger.warn("Library not found while listing items in SharePoint {}. {}", path, e);
            ClaFilePropertiesDto propsDto = ClaFilePropertiesDto.create().addError(
                    createScanError("Library not found while listing items in sharepoint",
                            e, path, runId));
            params.getFilePropertiesConsumer().accept(propsDto);
        }
    }

    @Override
    public MediaType getMediaType() {
        return MediaType.SHARE_POINT;
    }

    @Override
    public boolean isDirectoryExists(String path, boolean ignoreAccessErrors) {
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(path);
        try {
            String library = getLibraryName(key.getPath());
            if ("/".equals(library) || StringUtils.EMPTY.equals(library)) {
                getDocumentLibraries(null);
                return true;
            } else {
                int libInx = key.getPath().indexOf(library);
                if (libInx < 0) {
                    logger.warn("Could not locate library {} in path {} - return TRUE!", library, key.getPath());     // TODO - Oren?
                    return true;
                }
                String libPath = key.getPath().substring(libInx);
                if (isSubSitePath(key)) {
                    return getSubSites(key.getPath(), key.getSite());
                } else {
                    String libraryListId = getFolderProperties(null, libPath).getListId();
                    if (Strings.isEmpty(libraryListId)) {
                        return getSubSites(libPath, null);
                    }
                    return !Strings.isEmpty(libraryListId);
                }
            }
        } catch (FileNotFoundException e) {
            logger.warn("Failed to get file from SharePoint {} ", path, e);
            return false;
        }
    }

    private boolean getSubSites(String libPath, String subSite) {
        try {
            List<ServerResourceDto> sites = browseSubSiteFolders(libPath, subSite);
            return sites.size() > 0;
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("unused")
    public void getFileDetails(String subSite, String listId, String path) {
        try {
//            File file = service.getFile(path);
            //Find the list item that correlates with the path
            List<IQueryOption> queryOptions = new ArrayList<>();

            IFilterRestriction filterRestriction = new IsEqualTo("FieldRef/FileRef", path);
//            IFilterRestriction filterRestriction = new IsEqualTo("title",path);
            queryOptions.add(new Filter(filterRestriction));
            List<ListItem> listItems = execAsyncTask(() -> service.getListItems(subSite, listId, queryOptions));
            for (ListItem listItem : listItems) {
                logger.debug("Got list Item: {}", listItem);
            }
            //Now get the fields for the file/folder
            //Find the field value for RoleAssignments
//            File file = service.getFile(path);
//            List<Field> listFields = service.getListFields(file.getUniqueId());
//            for (Field listField : listFields) {
//                logger.debug(listField.toString());
//            }
        } catch (Exception e) {
            logger.error("Failed to fetch file content for {}", path, e);
            throw new RuntimeException("Failed to fetch file content for " + path, e);
        }
    }

    @SuppressWarnings("unused")
    public List<File> listFiles(String subSite, String basePath) throws Exception {
        List<File> files = execAsyncTask(() -> service.getFiles(subSite, basePath));
        if (files.size() > 0) {
            logger.debug("Got {} files from {}", files.size(), basePath);
        }
        return files;
    }

    @SuppressWarnings("unused")
    public List<ServerResourceDto> listFilesAsServerResources(String subSite, String path) {
        String parentFolder = calculateParentFolder(path);
        return listFilesAsServerResources(subSite, parentFolder, maxRetries);
    }

    @Override
    public List<ServerResourceDto> browseSubFolders(String folderMediaEntityId) {
        String path = Optional.ofNullable(folderMediaEntityId).orElse("/");
        logger.trace("List server folder {}", path);
        if ("/".equals(path)) {
            logger.trace("Show site");
            return listCoreSite(SharePointParseUtils.normalizePath(createBaseUri(true)) + "/", basePath);
        } else {
            logger.trace("Show folders under {}", path);
            List<ServerResourceDto> folders = browseSiteFolders(path);
            return folders.stream()
                    .sorted(Comparator.comparing(folder -> folder.getName().toLowerCase()))
                    .collect(Collectors.toList());
        }

    }

    private void streamFileScanDetailsFromSite(ConcurrentStreamData<StreamSiteData> concurrentStreamSiteData) {
        logger.debug("Stream files from each library under the site {}", concurrentStreamSiteData.getStreamData().getPath());

        streamSitesFolders(concurrentStreamSiteData);
        streamSubSites(concurrentStreamSiteData);
    }


    private void streamFileScanDetailsFromSite(StreamSiteData streamSiteData) {

        logger.debug("Stream files from each library under the site {}", streamSiteData.getPath());

        streamSitesFolders(streamSiteData);
        streamSubSites(streamSiteData);
    }

    private void streamSubSites(StreamSiteData streamSiteData) {
        if (siteCrawlMaxDepth != -1 && streamSiteData.getCurrentDepth() > siteCrawlMaxDepth - 1) {
            logger.warn("Reached max site depth path={}, depth={} (max={})", streamSiteData.getPath(), streamSiteData.getCurrentDepth(), siteCrawlMaxDepth);
            return;
        }

        logger.info("Starting to stream SUB-SITES for path {}", streamSiteData.getPath());
        List<ServerResourceDto> serverResourceDtos = listSubSites(MSItemKey.path(streamSiteData.getSubSite(), getLibraryName(streamSiteData.getPath())), basePath);
        serverResourceDtos
                .forEach(dto -> {
                    ScanTaskParameters scanParams = streamSiteData.getParams().getScanParams();
                    try {
                        StreamSiteData streamData = getStreamSiteData(streamSiteData, dto, scanParams);
                        streamFileScanDetailsFromSite(streamData);
                    } catch (Exception e) {
                        reportSubSiteStreamError(streamSiteData, dto, scanParams, e);
                    }
                });
    }

    private void streamSubSites(ConcurrentStreamData<StreamSiteData> conStreamSiteData) {
        StreamSiteData streamSiteData = conStreamSiteData.getStreamData();
        if (siteCrawlMaxDepth != -1 && streamSiteData.getCurrentDepth() > siteCrawlMaxDepth - 1) {
            logger.warn("Reached max site depth path={}, depth={} (max={})", streamSiteData.getPath(), streamSiteData.getCurrentDepth(), siteCrawlMaxDepth);
            return;
        }

        logger.info("Starting to stream SUB-SITES for path {}", streamSiteData.getPath());
        List<ServerResourceDto> serverResourceDtos = listSubSites(MSItemKey.path(streamSiteData.getSubSite(), getLibraryName(streamSiteData.getPath())), basePath);
        serverResourceDtos
                .forEach(dto -> {
                    ScanTaskParameters scanParams = streamSiteData.getParams().getScanParams();
                    try {
                        StreamSiteData streamData = getStreamSiteData(streamSiteData, dto, scanParams);
                        //noinspection unchecked
                        streamFileScanDetailsFromSite(new ConcurrentStreamData(conStreamSiteData, streamData));
                    } catch (Exception e) {
                        reportSubSiteStreamError(streamSiteData, dto, scanParams, e);
                    }
                });
    }

    private void reportSubSiteStreamError(StreamSiteData streamSiteData, ServerResourceDto dto, ScanTaskParameters scanParams, Exception e) {
        logger.error("Failed to scan library {} under core site.", dto, e);
        ClaFilePropertiesDto err = ClaFilePropertiesDto.create()
                .setFolder(true)
                .addError(createScanError("Failed to scan folder " + dto.getFullName(), e,
                        scanParams.getPath(), scanParams.getRunId()));
        streamSiteData.getParams().getFilePropertiesConsumer().accept(err);
    }

    private StreamSiteData getStreamSiteData(StreamSiteData streamSiteData, ServerResourceDto dto, ScanTaskParameters scanParams) {
        final String site = SharePointParseUtils.splitPathAndSubsite(dto.getFullName()).getSite();
        String siteFinal = Optional.ofNullable(streamSiteData.getSubSite())
                .map(sbSite -> site.startsWith(sbSite) ? site.substring(sbSite.length()) : site)
                .orElse(site);

        String normalizedPath = SharePointParseUtils.normalizePath(streamSiteData.getPath());
        String nextPath = SharePointParseUtils.splitPathAndSubsite(normalizedPath).getPath() + SharePointParseUtils.normalizePath(siteFinal);
        return new StreamSiteData(streamSiteData.getParams(), nextPath, site, streamSiteData.getCurrentDepth() + 1);
    }

    private void streamSitesFolders(ConcurrentStreamData<StreamSiteData> concurrentStreamSiteData) {
        StreamSiteData streamSiteData = concurrentStreamSiteData.getStreamData();

        logger.info("Starting to stream (concurrent) SITES FOLDERS for path {}", streamSiteData.getPath());

        List<ServerResourceDto> serverResourceDtos = listDocumentLibraries(streamSiteData.getSubSite());

        StreamListData streamListData;
        for (ServerResourceDto serverResourceDto : serverResourceDtos) {
            streamListData = processDocumentLibrary(streamSiteData, serverResourceDto);
            if (streamListData == null) {
                continue;
            }

            //noinspection unchecked
            streamFileScanDetailsFromList(new ConcurrentStreamData(concurrentStreamSiteData, streamListData));

        }
    }

    private void streamSitesFolders(StreamSiteData streamSiteData) {

        logger.info("Starting to stream SITES FOLDERS for path {}", streamSiteData.getPath());

        List<ServerResourceDto> serverResourceDtos = listDocumentLibraries(streamSiteData.getSubSite());

        StreamListData streamListData;
        for (ServerResourceDto serverResourceDto : serverResourceDtos) {
            streamListData = processDocumentLibrary(streamSiteData, serverResourceDto);
            if (streamListData == null) {
                continue;
            }

            streamFileScanDetailsFromList(streamListData);
        }
    }

    private StreamListData processDocumentLibrary(StreamSiteData streamSiteData, ServerResourceDto serverResourceDto) {
        String libName = null;
        try {
            String subSite = streamSiteData.getSubSite();
            String normalizedPath = Optional
                    .ofNullable(SharePointParseUtils.normalizePath(serverResourceDto.getFullName()))
                    .orElse(StringUtils.EMPTY);
            libName = Objects.requireNonNull(getLibraryName(normalizedPath, subSite));
            libName = SharePointParseUtils.normalizePath(libName);
            logger.info("Resolved lib name: {} subSite={} fullName={}", libName, serverResourceDto.getFullName(), subSite);
            if (libName != null && libName.startsWith(basePath)) {
                int fromIdx = Objects.requireNonNull(SharePointParseUtils.normalizePath(basePath)).length();
                if (subSite != null) {
                    fromIdx += Objects.requireNonNull(SharePointParseUtils.normalizePath(subSite)).length();
                }
                libName = libName.substring(fromIdx);
            }
            String tmpLibName = Optional.ofNullable(libName).orElse(StringUtils.EMPTY).toLowerCase();
            if (EXCLUDED_LIBS.stream().anyMatch(tmpLibName::endsWith) || tmpLibName.contains(EXCLUDED_LIB_PREFIX)) {
                return null;
            }

            String listId = serverResourceDto.getId();
            if (listId != null) {
                String listPath = Optional.ofNullable(subSite)
                        .map(site -> SharePointParseUtils.splitPathAndSubsite(streamSiteData.getPath()).getPath())
                        .orElse(streamSiteData.getPath());
                if (subSite != null && !listPath.contains(subSite)) {
                    listPath = SharePointParseUtils.applySiteMark(listPath, subSite);
                }
                listId = SharePointParseUtils.calculateMediaItemId(subSite, listId);
                String nextPath = SharePointParseUtils.removeUnneededDoubleSlashes(listPath + "/" + libName);
                String cacheEntry = addToFstLvlFoldersCache(nextPath);
                logger.trace("streamSitesFolders: add {} to fstLvlFoldersToConnectorMap", cacheEntry);
                nextPath = SharePointParseUtils.applySiteMark(nextPath, subSite);
                return new StreamListData(nextPath, listId, libName, subSite, streamSiteData.getParams());
            }
        } catch (Exception e) {
            logger.error("Failed to scan library {} under core site, continuing to the next. Resolved libName={}", serverResourceDto, libName, e);
            ScanErrorDto scanErrorDto = createScanError("Failed to scan folder " + serverResourceDto.getFullName(), e,
                    streamSiteData.getPath(), streamSiteData.getParams().getScanParams().getRunId());
            streamSiteData.getParams().getFilePropertiesConsumer().accept(ClaFilePropertiesDto.create()
                    .setFolder(true)
                    .addError(scanErrorDto));
        }
        return null;
    }

    private String addToFstLvlFoldersCache(@NotNull String nextPath) {
        String nextCachePath = URLDecoder.decode(nextPath);
        int subStrFromIdx = StringUtils.EMPTY.equals(basePath) || "/".equals(basePath)
                ? domainEndpoint.length() : nextCachePath.indexOf(basePath);
        String cacheEntry = domainEndpoint + nextCachePath.substring(subStrFromIdx).toLowerCase();
        fstLvlFoldersToConnectorMap.putIfAbsent(cacheEntry, this);
        return cacheEntry;
    }

    private void streamFileScanDetailsFromList(ConcurrentStreamData<StreamListData> concurrentStreamListData) {
        StreamListData streamListData = concurrentStreamListData.getStreamData();

        Map<String, Long> partAmounts = getPartAmounts(streamListData.getPath());
        if (shouldSkipAllFolder(streamListData.getPath(), partAmounts)) {
            return;
        }

        logger.info("Stream files from SharePoint list {}", streamListData.getListId());

        String mediaItemId = Optional.ofNullable(streamListData.getSubSite())
                .map(site -> SharePointParseUtils.calculateMediaItemId(site, streamListData.getListId()))
                .orElse(streamListData.getListId());

        Predicate<? super String> fileTypesPredicate = FileTypeUtils.createFileTypesPredicate(streamListData.getParams().getScanParams().getScanTypeSpecification());
        streamListData.getParams().setDirectoryListingConsumer(concurrentStreamListData.getDirectoryListingConsumer());
        streamListData.getParams().setFileTypesPredicate(fileTypesPredicate);

        MicrosoftRecursiveAction action = (MicrosoftRecursiveAction) MicrosoftRecursiveAction.Builder.create()
                .withMicrosoftConnectorBase(this)
                .withListId(SharePointParseUtils.splitMediaItemIdAndSite(mediaItemId).getListId())
                .withSubSite(streamListData.getSubSite())
                .withPath(streamListData.getPath())
                .withParams(streamListData.getParams())
                .build();

        concurrentStreamListData.getForkJoinPool().invoke(action);
    }

    private void streamFileScanDetailsFromList(StreamListData streamListData) {
        logger.info("Stream files from SharePoint list {}", streamListData.getListId());
        Predicate<? super String> fileTypesPredicate =
                FileTypeUtils.createFileTypesPredicate(streamListData.getParams().getScanParams().getScanTypeSpecification());

        streamListData.getParams().setFileTypesPredicate(fileTypesPredicate);

        streamFilesAndSubFolders(streamListData.getPath(),
                streamListData.getListId(),
                streamListData.getSubSite(),
                streamListData.getParams());
    }

    //------------------------

    @SuppressWarnings("unused")
    public List<ServerResourceDto> listFolders(String subSite, final String path) {
//        if (path != null && path.startsWith(LIBRARY_PREFIX)) {
//            parentFolder = path.replaceFirst(LIBRARY_PREFIX + ".+/?", ""); //Remove the library prefix and the library name
//            if (parentFolder.isEmpty()) {
//                parentFolder = basePath + "/";
//            }
//        }
//        String parentFolder = calculateParentFolder(path);
//        String parentFolder = path != null ? path : basePath + "/";

        logger.debug("List folders under sharePoint parent: {}", path);
        try {
            List<Folder> folders = execAsyncTask(() -> service.getFolders(subSite, path));
            List<ServerResourceDto> result = new ArrayList<>();
            for (Folder folder : folders) {
                String name = folder.getName();
                if (StringUtils.isEmpty(name) || name.startsWith("_")) {
                    //Internal folder
                    continue;
                }
                String fullName = path + "/" + name;
                ServerResourceDto serverResourceDto = new ServerResourceDto(fullName, name);
                serverResourceDto.setHasChildren(folder.getItemCount() > 0);
                serverResourceDto.setType(ServerResourceType.FOLDER);
                result.add(serverResourceDto);

            }

            return result;
        } catch (Exception e) {
            logger.error("Failed to list folders under sharePoint connector", e);
            throw new MediaConnectionException("Failed to list folders under sharePoint connector: " + e.getMessage(), BadRequestType.OPERATION_FAILED);
        }
    }

    private String calculateParentFolder(String path) {
        if (path == null && basePath == null) {
            return "/";
        }
        if (path != null && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return basePath != null ? basePath + path : path;
    }


    protected String convertFileNameIfNeeded(String fileName) {
        if (fileName != null && fileName.startsWith("http")) {
            //Standard fileNames include the full URL
            //http://ec2-54-200-41-63.us-west-2.compute.amazonaws.com/sites/test/small library/NewCo Needs
            int length = calculateBaseUriLength(false);
            fileName = fileName.substring(length);
        }
        return fileName;
    }

    @SuppressWarnings("unused")
    public ClaFilePropertiesDto getFolderAttributes(String subSite, String path, boolean fetchAcls) throws FileNotFoundException {
        //http://ec2-54-200-41-63.us-west-2.compute.amazonaws.com/sites/test/small library
        //Let's understand first if it's a library or a folder.
        ClaFilePropertiesDto claFilePropertiesDto = ClaFilePropertiesDto.create();
        claFilePropertiesDto.setFileName(path);
        claFilePropertiesDto.setFolder(true);
        int baseUriLength = calculateBaseUriLength(true);
//        String baseUri = createBaseUri();
        String nameSuffix = path.substring(baseUriLength + 1);
        try {
            if (nameSuffix.isEmpty()) {
                logger.debug("get core site attributes");
                Site site = execAsyncTask(() -> service.getSite(subSite));
                claFilePropertiesDto.setMediaItemId("site:" + site.getId());
            } else if (nameSuffix.contains("/")) {
                //http://ec2-54-200-41-63.us-st-2.compute.amazonaws.com/sites/test/_api/Web/GetFolderByServerRelativeUrl('/sites/test/Shared%20Documents/personal')/ListItemAllFields?$select=id
                String folderMediaItemId = microsoftDocAuthorityClient.getFolderMediaItemId(null, nameSuffix);
                claFilePropertiesDto.setMediaItemId(folderMediaItemId);
                if (fetchAcls) {
                    fetchAcls(claFilePropertiesDto);
                }
            } else {
                //Library - convert nameSuffix to title.
                String libraryListId = getFolderProperties(null, nameSuffix).getListId();
                claFilePropertiesDto.setMediaItemId(libraryListId);
                if (fetchAcls) {
                    List<SharePointRoleAssignment> listPermissions = microsoftDocAuthorityClient.getListPermissions(null, libraryListId);
                    logger.debug("List (library: {} ) has {} permissions", nameSuffix, listPermissions.size());
                    addListItemPermissionsToClaFileProperties(claFilePropertiesDto, listPermissions);
                    claFilePropertiesDto.calculateAclSignature();
                }
            }
        } catch (Exception e) {
            logger.error("Failed to find library {} under sharePoint", nameSuffix, e);
            throw new FileNotFoundException("Failed to find library " + nameSuffix + " under sharePoint: " + e.getMessage());
        }
        return claFilePropertiesDto;
    }

    public ClaFilePropertiesPageDto listItems(String listId, int count, String pageIdentifier, String pathPrefix) {
        try {
            long start = System.currentTimeMillis();
            SharePointListItemPage listItemsPage;
            if (pageIdentifier != null) {
                //This is the URL to send - sanitize it first
                logger.debug("List items using Url: {}", pageIdentifier);
                pageIdentifier = pageIdentifier.substring(microsoftDocAuthorityClient.createApiUri(StringUtils.EMPTY, StringUtils.EMPTY).length());
                listItemsPage = microsoftDocAuthorityClient.getListItems(pageIdentifier);
            } else {
                List<IQueryOption> queryOptions = Lists.newArrayList();
                //http://ec2-54-200-41-63.us-west-2.compute.amazonaws.com/sites/test/_api/web/lists('f1f04276-593b-454b-8ee1-006f83af18d3')
                // /Items?$top=5&$expand=FieldValuesAsText/fileref&$filter=startswith(FileRef,%20%27/sites%27)
                queryOptions.add(new SharepointPagingToken(null, count));
                if (!sharePointOnline && pathPrefix != null) {
                    queryOptions.add(new Filter(new SharePointStartsWithRestriction("FileRef", pathPrefix)));
                }
                queryOptions.add(new OrderBy(new PropertyOrder("ID")));
                microsoftDocAuthorityClient.addDefaultListItemQueryOptions(queryOptions);
                listItemsPage = microsoftDocAuthorityClient.getListItems(listId, queryOptions);
            }
            List<SharePointListItem> listItems = listItemsPage.getItems();
            long duration = System.currentTimeMillis() - start;
            String nextUrl = listItemsPage.getNextUrl();
            List<ClaFilePropertiesDto> claFilePropertiesDtos = convertSharePointListItemsToFiles(listId, listItems);
            if (pageIdentifier != null) {
                logger.debug("List up to {} items from url {} got {} items (pathPrefix={}) in {} ms.\nNext Page: {}",
                        count, pageIdentifier, claFilePropertiesDtos.size(), pathPrefix, duration, nextUrl);
            } else {
                logger.debug("List up to {} items from the beginning got {} items (pathPrefix={}) in {} ms.\nNext Page: {}",
                        count, claFilePropertiesDtos.size(), pathPrefix, duration, nextUrl);
            }
            return new ClaFilePropertiesPageDto(claFilePropertiesDtos, nextUrl);
        } catch (ServiceException e) {
            throw new RuntimeException("Failed to list items from list " + listId, e);
        }
    }

    private List<ClaFilePropertiesDto> convertSharePointListItemsToFiles(String listId, List<SharePointListItem> sharePointListItems) {
        List<ClaFilePropertiesDto> filePropertiesDtos = new ArrayList<>();
        for (SharePointListItem listItem : sharePointListItems) {
            ClaFilePropertiesDto filePropertyDto = convertToClaFilePropertiesDto(null, listId, listItem);
            filePropertiesDtos.add(filePropertyDto);
        }

        return filePropertiesDtos;
    }

    @SuppressWarnings("unused")
    public List<ClaFilePropertiesDto> convertListItemsToFiles(String subSite, String listId, List<ListItem> listItems) throws Exception {
        List<ClaFilePropertiesDto> filePropertiesDtos = new ArrayList<>();
        for (ListItem listItem : listItems) {
            List<FieldValue> fieldValues = execAsyncTask(() -> service.getFieldValues(subSite, listId, listItem.getId()));
            Map<String, FieldValue> fieldValuesMap = fieldValues.stream()
                    .collect(Collectors.toMap(FieldValue::getName, Function.identity()));
            FieldValue fileRef = fieldValuesMap.get("FileRef");
            //Created_x005f_x0020_x005f_By, Id (listItemId), Created_x005f_x0020_x005f_Date     7/26/2016 1:47 PM
            //Author ,Modified    7/26/2016 1:47 PM, Last_x005f_x0020_x005f_Modified    7/26/2016 1:47 PM
            //getListItemPermissions(listId, listItem);
            boolean listItemHavingUniqueAcls = isListItemHavingUniqueAcls(subSite, listId, listItem);
            ClaFilePropertiesDto filePropertyDto = ClaFilePropertiesDto.create();
            filePropertyDto.setFileName(microsoftDocAuthorityClient.convertFileRefToFileUrl(fileRef.getValue()));
            filePropertyDto.setMediaItemId(listId + "/" + listItem.getId());
            AclInheritanceType aclInheritanceType = listItemHavingUniqueAcls ? AclInheritanceType.NONE : AclInheritanceType.FOLDER;
            filePropertyDto.setAclInheritanceType(aclInheritanceType);
            if (FileSystemObjectType.FOLDER.equals(listItem.getFileSystemObjectType())) {
                filePropertyDto.setFolder(true);
            }
            filePropertiesDtos.add(filePropertyDto);
        }
        return filePropertiesDtos;
    }

    private boolean isListItemHavingUniqueAcls(String subSite, String listId, ListItem listItem) throws Exception {
        String hasUniqueRoleAssignments = execAsyncTask(() ->
                service.getListItemProperty(subSite, listId, listItem.getId(), "HasUniqueRoleAssignments"));
        return hasUniqueRoleAssignments.contains(">true</d:HasUniqueRoleAssignments>");
    }

    /*public void getFilePermissions(String fileName) throws ServiceException {
        //.../GetFileByServerRelativeUrl('')?$expand=ListItemAllFields/RoleAssignments/Member,ListItemAllFields/RoleAssignments/RoleDefinitionBindings,ListItemAllFields/RoleAssignments/Member/Users
        String s1 = Util.encodeEscapeCharacters(fileName);
        String url = "_api/web/GetFileByServerRelativeUrl('" + s1 + "')?$expand=ListItemAllFields/RoleAssignments";
        InputStream inputStream = service.getInputStream(url);
        try {
            String s = IOUtils.toString(inputStream, Charset.defaultCharset());
            logger.debug("Got result: {}", s);
//            DocumentBuilder documentBuilder = xmlDocumentBuilder.newDocumentBuilder();
//            Document document = documentBuilder.parse(inputStream);

        } catch (Exception e) {
            throw new ServiceException("Failed to parse XML", e, url);
        }

    }*/

    public String getLastChange(String subSite, String listId) throws Exception {
        ChangeQuery query = createItemChangeQuery();

        List<IQueryOption> queryOptions = new ArrayList<>();
        queryOptions.add(new Top(1));
        queryOptions.add(new OrderBy(new PropertyOrder("time", true)));

        ChangeToken lastChangeToken = null;
        int i = 0;
        while (i < 1000) {
            if (lastChangeToken != null) {
                query.setChangeTokenStart(lastChangeToken);
            }
            List<Change> changes = execAsyncTask(() -> service.getChanges(subSite, query, listId, queryOptions));
            if (changes.size() == 0) {
                logger.debug("No more changes after {} calls", i);
                break;
            }
            Change change = changes.get(0);
            logger.debug("Change Token: {}", change.getToken());
            if (lastChangeToken != null && change.getToken().getChangeNumber() < lastChangeToken.getChangeNumber()) {
                logger.debug("Change Token: {} is smaller then last one: {} after {} calls", change.getToken(), lastChangeToken, i);
                break;
            } else {
                lastChangeToken = change.getToken();
            }
            i++;
        }
        logger.debug("done after {} calls", i);
        return SharePointParseUtils.convertSharePointChangeToString(lastChangeToken);
    }

    private ChangeQuery createItemChangeQuery() {
        ChangeQuery query = new ChangeQuery();
//        query.setFile(true);
//        query.setFolder(true);
        query.setItem(true);
        query.setDelete(true);
        query.setMove(true);
        query.setAdd(true);
        query.setUpdate(true);
        query.setRestore(true);
        query.setRename(true);
        query.setRoleAssignmentAdd(true);
        query.setRoleAssignmentDelete(true);
        return query;
    }

    public List<MediaChangeLogDto> getChanges(String subSite, String listId, String changeTokenStartJson, int count) throws Exception {
        if (listId == null) {
            throw new MediaConnectionException("listId", "null", BadRequestType.MISSING_FIELD);
        }
        ChangeQuery query = createItemChangeQuery();
        if (changeTokenStartJson != null) {
            SharePointChangeTokenDto changeTokenDto = SharePointParseUtils.convertToSharePointChangeToken(changeTokenStartJson);
            Date changeTime = new Date(changeTokenDto.getDate());
            query.setChangeTokenStart(new ChangeToken(ChangeTokenScope.LIST, changeTokenDto.getScopeId(), changeTime, changeTokenDto.getChangeNumber()));
        }
        List<IQueryOption> queryOptions = new ArrayList<>();
        queryOptions.add(new Top(count));
        List<Change> changes = execAsyncTask(() -> service.getChanges(subSite, query, listId, queryOptions));
        logger.debug("Got {} changes", changes.size());
        return SharePointParseUtils.convertToMediaChangeLogDtos(listId, changes);
    }


    public Stream<MediaChangeLogDto> streamSharePointChanges(String subSite, String listId, String startingToken) throws Exception {
        List<MediaChangeLogDto> changes = getChanges(subSite, listId, startingToken, 1000);
        boolean readNextPage = changes.size() == 1000;
        if (readNextPage) {
            String nextToken = changes.get(999).getChangeLogPosition();
            Stream<MediaChangeLogDto> sharePointChangeStream = streamSharePointChanges(subSite, listId, nextToken);

            return Stream.concat(changes.stream(), sharePointChangeStream);
        } else {
            return changes.stream();
        }
    }

    @SuppressWarnings("unused")
    public List<Change> getListItemChanges(String subSite, String listId, int count, ChangeToken token) throws Exception {
        ChangeLogItemQuery query = new ChangeLogItemQuery();
//        query.setRowLimit(count);
        query.setToken(token);
        CamlQueryOptions queryOptions = new CamlQueryOptions();
        queryOptions.setIncludeMandatoryColumns(true);
        queryOptions.setIncludePermissions(true);
        query.setQueryOptions(queryOptions);
        return execAsyncTask(() -> service.getListItemChanges(subSite, listId, query));
    }

    @SuppressWarnings("unused")
    public com.middleware.share.List getListByTitle(String subSite, String listName) throws FileNotFoundException {
        try {
            com.middleware.share.List listByTitle = execAsyncTask(() -> service.getListByTitle(subSite, listName));
            String entityTypeName = listByTitle.getEntityTypeName();
            logger.debug("Got list [{}] attached to entity [{}]", listName, entityTypeName);
            return listByTitle;
        } catch (Exception e) {
            logger.error("Failed to get list: {}", listName, e);
            throw new FileNotFoundException("Failed to get list: " + listName);
        }
    }

    private List<com.middleware.share.List> getDocumentLibraries(String subSite) {
        try {

            String subSiteStr = SharePointParseUtils.encodeSubSiteNameIfNeeded(subSite);

            List<IQueryOption> queryOptions = new ArrayList<>();
            IFilterRestriction filterRestriction = new IsEqualTo("baseType", ListBaseType.DOCUMENT_LIBRARY.ordinal());
            queryOptions.add(new Filter(filterRestriction));
            return execAsyncTask(() -> service.getLists(subSiteStr, queryOptions));
        } catch (Exception e) {
            logger.error("Failed to get lists (subSite=" + subSite + ")", e);
            throw new MediaConnectionException("Failed to get lists (" + e.getMessage() + ")", BadRequestType.OPERATION_FAILED);
        }
    }

    protected List<ServerResourceDto> listDocumentLibraries(String subSite) {
        logger.debug("List sharepoint document libraries");
        List<com.middleware.share.List> lists = getDocumentLibraries(subSite);
        List<ServerResourceDto> result = Lists.newArrayList();

        for (com.middleware.share.List list : lists) {
            String libraryBasePath = extractLibraryBasePath(subSite, list);
            ServerResourceDto serverResourceDto = new ServerResourceDto();
            serverResourceDto.setType(ServerResourceType.LIBRARY);
            serverResourceDto.setId(list.getId());
            String title = list.getTitle(); //Documents
            title = SharePointParseUtils.parseInternalName(title); //Documents

            serverResourceDto.setName(title);
            String siteUrlPart = Optional.ofNullable(subSite)
//                    .map(val -> val + "/")
                    .orElse(StringUtils.EMPTY);
            if (libraryBasePath.toLowerCase().contains(EXCLUDED_LIB_PREFIX)) {
                continue;
            }
            if (!libraryBasePath.startsWith("/")) {
                libraryBasePath = "/" + libraryBasePath;
            }
            //http://ec2-54-200-41-63.us-west-2.compute.amazonaws.com/sites/test/small library
            String fullName = SharePointParseUtils.normalizePath(createBaseUri(true) + "/" + siteUrlPart + libraryBasePath);
            serverResourceDto.setFullName(fullName);
            logger.debug("List entity (library): {} fullName: {} id: {}", libraryBasePath, fullName, list.getId());
            result.add(serverResourceDto);
        }
        return result;

    }

    @Override
    protected String extractLibraryBasePath(String subSite, com.middleware.share.List list) {
        String libraryBasePath = list.getEntityTypeName(); ////Shared_x0020_Documents
        libraryBasePath = SharePointParseUtils.parseInternalName(libraryBasePath); //Shared Documents
        String documentTemplateUrl = list.getDocumentTemplateUrl();
        if (StringUtils.isNotEmpty(documentTemplateUrl) && StringUtils.isEmpty(basePath)) {
            if (subSite != null) {
                subSite = SharePointParseUtils.normalizePath(subSite);
                if (documentTemplateUrl.toLowerCase().startsWith(subSite.toLowerCase())) {
                    documentTemplateUrl = documentTemplateUrl.substring(subSite.length());
                }
            }

            int stopPoint = documentTemplateUrl.indexOf('/');
            int startPoint = 0;
            if (stopPoint == 0) {
                stopPoint = documentTemplateUrl.indexOf('/', 1);
                startPoint = 1;
            }

            if (stopPoint < 0) {
                logger.debug("Document template URL is not helping to get library base path: {}", documentTemplateUrl);
            } else {
                libraryBasePath = documentTemplateUrl.substring(startPoint, stopPoint);
            }
        } else if (StringUtils.isNotEmpty(documentTemplateUrl) && documentTemplateUrl.startsWith(basePath)) { // /tests/Shared Documents/Forms/template.dotx
            String path = basePath;
            if (subSite != null) {
                if (!path.endsWith("/") && !subSite.startsWith("/")) {
                    path += "/";
                }
                path += subSite;
            }

            int stopPoint = documentTemplateUrl.indexOf('/', path.length() + 1);
            libraryBasePath = documentTemplateUrl.substring(path.length(), stopPoint);
        }
        return libraryBasePath;
    }

    /**
     * 25 16:23:52,858 DEBUG [main] SharePointMediaConnector:447 - User ID: 1, Login Name: i:0#.w|docauthority\\administrator
     * 25 16:23:52,858 DEBUG [main] SharePointMediaConnector:447 - User ID: 4, Login Name: c:0(.s|true
     * 25 16:23:52,858 DEBUG [main] SharePointMediaConnector:447 - User ID: 1073741823, Login Name: SHAREPOINT\\system
     * 25 16:23:52,858 DEBUG [main] SharePointMediaConnector:447 - User ID: 11, Login Name: i:0#.w|docauthority\\testsharepoint
     * 25 16:23:52,859 DEBUG [main] SharePointMediaConnector:447 - User ID: 8, Login Name: i:0#.w|docauthority\\uri.shtand
     */
    public void listPrincipals() {
        try {
            List<User> users = execAsyncTask(() -> service.getUsers(null));
            for (User user : users) {
                logger.debug("User ID: {}, Login Name: {}", user.getId(), user.getLoginName());
            }

        } catch (Exception e) {
            throw new RuntimeException("Unable to extract User lists", e);
        }
    }

    private String getLibraryName(final String path) {
        return getLibraryName(path, null);
    }

    private String getLibraryName(final String path, final String subSite) {
        String basePath = this.basePath;
        String library = path;
        String librarySuffix;
        if (path.startsWith("http")) {
            int i = calculateBaseUriLength(true);
            int subSiteStrLen = Optional.ofNullable(subSite)
                    .map(site -> SharePointParseUtils.normalizePath(site).length() - (basePath.endsWith("/") ? 1 : 0))
                    .orElse(0);
            i += subSiteStrLen;
            if (i == path.length() + 1 && basePath.endsWith("/")) { // path might missing last '/'
                i--;
            }
            librarySuffix = SharePointParseUtils.normalizePath(path).substring(i);
            // if suffix starts with one or more "/" - remove them
            librarySuffix = librarySuffix.replaceAll("^[/]+", StringUtils.EMPTY);

            if (librarySuffix.contains("/")) {
                library = StringUtils.substringBefore(librarySuffix, "/");
            } else {
                library = librarySuffix;
            }
        } else {
            logger.warn("Library {} name is probably illegal", library);
            String[] split = library.split("/");
            return split[split.length - 1];
        }
        library = SharePointParseUtils.normalizePath(library);
        if (subSite != null) {
            library = SharePointParseUtils.normalizePath(subSite) + library;
        }
        String result = SharePointParseUtils.normalizePath(basePath) + library;
        logger.debug("Extracted library name: {} from {}", result, librarySuffix);
        return SharePointParseUtils.normalizePath(result);
    }

    @Override
    protected String createBaseUri(boolean includeBasePath) {
        return includeBasePath ? baseUriWithBasePath : baseUri;
    }

    private int calculateBaseUriLength(boolean includeBasePath) {
        return includeBasePath ? baseUriWithBasePath.length() : baseUri.length();
    }

    private List<ServerResourceDto> browseSiteFolders(String path) {
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(path);
        String itemPath = key.getPath();
        String site = key.getSite();
        String baseUri = createBaseUri(false);
        String updatedPath = itemPath.startsWith(baseUri) ? itemPath.substring(baseUri.length()) : path;
        logger.debug("List site folders/libraries at {} (from {})", updatedPath, path);
        try {
            List<ServerResourceDto> result;
            if (site != null && detectedSubSites.contains(key.getPath())
                    && (itemPath.endsWith(site) || itemPath.endsWith(site + "/"))) {
                result = browseSubSiteFolders(updatedPath, site);
                result.addAll(listSubSites(key, basePath));
            } else {
                result = browseFolders(site, path, updatedPath, baseUri);
            }

            addSubSitesIfNeeded(path, updatedPath, result);
            return result;
        } catch (Exception e) {
            logger.error("Failed to list sharePoint folders at {}", path, e);
            throw new RuntimeException("Failed to list sharePoint folders at " + path + " (" + e.getMessage() + ")", e);
        }
    }

    private void addSubSitesIfNeeded(String path, String updatedPath, List<ServerResourceDto> result) {
        String pathUri = SharePointParseUtils.normalizePath(path);
        String conUri = SharePointParseUtils.normalizePath(baseUriWithBasePath);
        if (pathUri.equalsIgnoreCase(conUri)) {
            result.addAll(listSubSites(updatedPath));
        }
    }

    @Override
    protected List<ServerResourceDto> listSubSites(String updatedPath) {
        return listSubSites(SharePointParseUtils.splitPathAndSubsite(updatedPath), basePath);
    }

    @Override
    protected List<ServerResourceDto> browseSubSiteFolders(String relPath, String subSiteOpt) throws Exception {
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(relPath);
        final String subSite = Optional.ofNullable(subSiteOpt)
                .orElse(key.getSite());

        List<ServerResourceDto> folders = execAsyncTask(() -> microsoftDocAuthorityClient.listSubSitesFolders(subSite, key.getPath()));
        final int allSize = folders.size();
        folders = folders.stream()
                .filter(folder -> !EXCLUDED_LIBS.contains("/" + folder.getName()) && !folder.getName().startsWith("_"))
                .peek(folder -> {
                    String fullName = SharePointParseUtils.removeUnneededDoubleSlashes(folder.getFullName());
                    fullName = SharePointParseUtils.applySiteMark(fullName, subSite);
                    folder.setFullName(fullName);
                })
                .collect(Collectors.toList());
        logger.debug("Got {} sub-site's folders (out of {} optional folders)", folders.size(), allSize);
        return folders;
    }

    @NotNull
    private List<ServerResourceDto> browseFolders(String subSite, String path, String updatedPath, String baseUri) throws Exception {
        List<SharePointExtendedFolder> sharePointExtendedFolders = execAsyncTask(() -> microsoftDocAuthorityClient.listFolders(subSite, updatedPath));
        logger.trace("Acquired {} folders under {}", sharePointExtendedFolders.size(), path);
        List<ServerResourceDto> result = new ArrayList<>();
        for (SharePointExtendedFolder sharePointExtendedFolder : sharePointExtendedFolders) {
            if (EXCLUDED_LIBS.contains("/" + sharePointExtendedFolder.getName().toLowerCase()) && !sharePointExtendedFolder.getName().startsWith("_")) {
                continue;
            }
            String name = sharePointExtendedFolder.getName();
            if (!isFolderFiltered(name)) {
                String serverRelativeUrl = SharePointParseUtils.removeUnneededDoubleSlashes(baseUri + sharePointExtendedFolder.getServerRelativeUrl());
                if (subSite != null) {
                    serverRelativeUrl = SharePointParseUtils.applySiteMark(serverRelativeUrl, subSite);
                }
                ServerResourceDto serverResourceDto = new ServerResourceDto(serverRelativeUrl, name);
                if (sharePointExtendedFolder.getFolderItemCount() != null &&
                        sharePointExtendedFolder.getFolderItemCount() == 0) {
                    serverResourceDto.setHasChildren(false);
                } else {
                    serverResourceDto.setHasChildren(true);
                }
                if (sharePointExtendedFolder.getListTitle() != null) {
                    String title = SharePointParseUtils.parseInternalName(sharePointExtendedFolder.getListTitle());//Documents
                    serverResourceDto.setName(title);
                }
                result.add(serverResourceDto);
            }
        }
        logger.debug("Got {} folders (out of {} optional folders)", result.size(), sharePointExtendedFolders.size());
        return result;
    }


    private void extractParametersFromUrl(String url) {
        URI uri;
        try {
            URL url1 = new URL(SharePointParseUtils.removeUnneededDoubleSlashes(url));
            uri = url1.toURI();
        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException("Malformed URL for SharePoint connector " + url);
        }

        host = uri.getHost();
        port = uri.getPort() != -1 ? Integer.toString(uri.getPort()) : null;
        basePath = SharePointParseUtils.normalizePath(uri.getPath());
        scheme = uri.getScheme();
        domainEndpoint = scheme + "://" + host
                + Optional.ofNullable(port)
                .map(p -> ":" + p)
                .orElse("");
        domainEndpoint = SharePointParseUtils.normalizePath(domainEndpoint.toLowerCase());
    }

    @Override
    public void streamMediaChangeLog(StreamMediaItemsParams params) {
        String basePathAddendum = resolveRootFolderBasePath(SharePointParseUtils.splitPathAndSubsite(params.getRealPath()), false);
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(params.getRealPath());
        if (StringUtils.EMPTY.equals(basePathAddendum)) {
            super.streamMediaChangeLogForSite(params.getScanParams(), key.getPath(), key.getSite(),
                    params.getStartChangeLogPosition(), params.getChangeConsumer());
        } else {
            SharePointMediaConnector conn = recreateConnectorWithAdjustedParams(basePathAddendum);
            Consumer<MediaChangeLogDto> basePathAdjustedConsumer = dto -> {
                String mediaItemId = SharePointParseUtils.applyBasePathCompletionToMediaItemId(basePathAddendum, dto.getMediaItemId());
                dto.setMediaItemId(mediaItemId);
                params.getChangeConsumer().accept(dto);
            };
            conn.streamMediaChangeLogForSite(params.getScanParams(), params.getRealPath(), key.getSite(),
                    params.getStartChangeLogPosition(), basePathAdjustedConsumer);
        }

    }

    private String resolveRootFolderBasePath(MSItemKey key, boolean isFile) {
        if (key.getSite() == null) {
            return resolveRootFolderBasePath(key.getPath(), isFile);
        }

        String path = key.getPath();
        int subStrUpToIdx = path.indexOf(key.getSite());
        return resolveRootFolderBasePath(path.substring(0, subStrUpToIdx), isFile);
    }

    private String resolveRootFolderBasePath(String fullPath, boolean isFile) {
        String rfRelativePath = resolveObjectRelativePath(fullPath, isFile);
        String[] subPaths = rfRelativePath.split("/");
        for (int partIdx = subPaths.length - 1; partIdx > -1; partIdx--) {
            String currentPath = subPaths[partIdx];
            if (StringUtils.isEmpty(currentPath)) {
                continue;
            }
            rfRelativePath = rfRelativePath.substring(0, rfRelativePath.lastIndexOf("/" + currentPath) + currentPath.length() + 1); //+1 for: "/" + currentPath
            String sitePath = domainEndpoint + SharePointParseUtils.normalizePath(basePath + "/" + rfRelativePath).toLowerCase();
            if (basePathToConnectorMap.get(sitePath) != null || microsoftDocAuthorityClient.isValidSubSiteEndpoint(rfRelativePath)) {
                logger.debug("resolveRootFolderBasePath: Resolved sub-path: {}", rfRelativePath);
                return rfRelativePath;
            }
        }

        logger.debug("resolveRootFolderBasePath: No sub-paths were resolved for {}, isFile={}", fullPath, isFile);
        return StringUtils.EMPTY;
    }

    /**
     * @param fullPath Full path
     * @param isFile   file/folder
     * @return Resolved relative path, or "" (StringUtils.EMPTY)
     */
    private String resolveObjectRelativePath(String fullPath, boolean isFile) {
        if (isFile) {
            fullPath = fullPath.substring(0, fullPath.lastIndexOf("/"));
        }
        fullPath = Optional.ofNullable(SharePointParseUtils.normalizePath(fullPath)).orElse(StringUtils.EMPTY);//.replaceAll("[{}]+", "")

        String connUrl = url;
        if (!fullPath.toLowerCase().startsWith(connUrl.toLowerCase())) {
            logger.warn("Root folder path {} doesn't seem to be rooted from connection url {}", fullPath, connUrl);
            return StringUtils.EMPTY;
        }
        return fullPath.substring(connUrl.length());
    }

    @Override
    public FileContentDto getFileContent(FileContentParams params) throws FileNotFoundException {
        long start = timeSource.currentTimeMillis();
        try {
            MSItemKey itemKey = SharePointParseUtils.splitMediaItemIdAndSite(params.getFilename());
            SharePointMediaConnector conn = getBasePathCompatibleConnectorForSubSitePath(itemKey);
            return conn.getFileContentInner(params.getFilename(), true, params.isForUserDownload());
        } finally {
            logger.debug("Function execution time={} millis", timeSource.millisSince(start));
        }
    }

    @NotNull
    private String getFileRelativePathToHost(String filename) {
        if (filename.startsWith("http")) {
            int i = calculateBaseUriLength(true);
            filename = filename.substring(i);
        }
        return filename;
    }

    @Override
    public ClaFilePropertiesDto getFileAttributes(FileContentParams params) throws FileNotFoundException {
        long start = timeSource.currentTimeMillis();
        try {
            MSItemKey itemKey = SharePointParseUtils.splitMediaItemIdAndSite(params.getFilename());
            SharePointMediaConnector conn = getBasePathCompatibleConnectorForSubSitePath(itemKey);
            return conn.getFileAttributes(params.getFilename(), true);
        } finally {
            logger.debug("Function execution time={} millis", timeSource.millisSince(start));
        }
    }

    public InputStream getInputStream(String mediaItemId) throws FileNotFoundException {
        long start = timeSource.currentTimeMillis();
        try {
            MSItemKey itemKey = SharePointParseUtils.splitMediaItemIdAndSite(mediaItemId);
            SharePointMediaConnector conn = getBasePathCompatibleConnectorForSubSitePath(itemKey);
            return conn.getInputStreamForMediaItemId(mediaItemId);
        } finally {
            logger.debug("Function execution time={} millis", timeSource.millisSince(start));
        }
    }

    @NotNull
    private SharePointMediaConnector getBasePathCompatibleConnectorForSubSitePath(MSItemKey itemKey) {
        return Optional.ofNullable(itemKey.getBasePathAddendum())
                .map(this::recreateConnectorWithAdjustedParams)
                .orElse(this);
    }

    /**
     * Use this to filename without sub-sites!
     * For sub-sites use {@link SharePointMediaConnector#getBasePathCompatibleConnectorForSubSitePath}
     */
    @NotNull
    private SharePointMediaConnector getBasePathCompatibleConnector(String filename, String folderPath) {
        SharePointMediaConnector conn;
        String folder = getFileRelativePathToHost(folderPath);
        conn = getCachedCompatibleConnectorForFolder(folder);
        if (conn == null) {
            folder = SharePointParseUtils.normalizePath(basePath + "/" + folder);
            String basePathCompletion = resolveRootFolderBasePath(filename, true);
            logger.trace("getBasePathCompatibleConnector: Resolved basePathCompletion={} for filename={}", basePathCompletion, filename);
            conn = getOrCreateCachedConnectorForRootSite(basePathCompletion);
            logger.trace("getBasePathCompatibleConnector: Substring folder {}, conn.basePath={} for filename={}", folder, conn.basePath, filename);
            folder = folder.substring(
                    folder.toLowerCase().indexOf(conn.basePath) + conn.basePath.length() + 1);
            if (folder.startsWith("/")) {
                folder = folder.substring(1);
            }
            if (folder.contains("/")) {
                folder = folder.substring(0, folder.indexOf("/"));
            }

            String normalizedPath = Optional.ofNullable(
                    SharePointParseUtils.normalizePath(basePath + "/" + basePathCompletion + "/" + folder))
                    .orElse(StringUtils.EMPTY)
                    .toLowerCase();
            String cacheKey = domainEndpoint + normalizedPath;
            logger.trace("getBasePathCompatibleConnector: Registering connector under {} with fstLvlFoldersToConnectorMap", cacheKey);
            fstLvlFoldersToConnectorMap.put(cacheKey, conn);
        }
        return conn;
    }

    private SharePointMediaConnector getCachedCompatibleConnectorForFolder(String folderRelPath) {
        folderRelPath = SharePointParseUtils.normalizePath(folderRelPath.toLowerCase());
        logger.trace("getCachedCompatibleConnectorForFolder: Looking for cached connector for folder {}", folderRelPath);
        String cacheKey = Optional.ofNullable(SharePointParseUtils.normalizePath(domainEndpoint + basePath + "/" + folderRelPath))
                .orElse("_no_such_key_").toLowerCase();
        SharePointMediaConnector conn = fstLvlFoldersToConnectorMap.get(cacheKey);
        if (conn != null) {
            logger.trace("getCachedCompatibleConnectorForFolder: Found connector for {}, cacheKey={}", folderRelPath, cacheKey);
            return conn;
        } else {
            logger.trace("getCachedCompatibleConnectorForFolder: Could not find connector for {}, cacheKey={}", folderRelPath, cacheKey);
        }
        String[] subPaths = folderRelPath.split("/");
        for (int partIdx = subPaths.length - 1; partIdx > -1 && !basePath.equalsIgnoreCase(folderRelPath); partIdx--) {
            String currentPath = subPaths[partIdx];
            if (StringUtils.isEmpty(currentPath)) {
                continue;
            }
            folderRelPath = folderRelPath.substring(0, folderRelPath.lastIndexOf("/" + currentPath));
            String iterCacheKey = domainEndpoint + SharePointParseUtils.normalizePath(basePath + "/" + folderRelPath);
            conn = fstLvlFoldersToConnectorMap.get(iterCacheKey);
            if (conn != null) {
                logger.trace("getCachedCompatibleConnectorForFolder: Resolved compatible media-connector for folder: {}, cacheKey={}", folderRelPath, iterCacheKey);
                return conn;
            }
            logger.trace("getCachedCompatibleConnectorForFolder: No cached connector found for path {}, cacheKey={}", folderRelPath, iterCacheKey);
        }
        return null;
    }

    @NotNull
    private SharePointMediaConnector getOrCreateCachedConnectorForRootSite(String siteAddendum) {
        /*String site = domainEndpoint + SharePointParseUtils.normalizePath(basePath + "/" + siteAddendum).toLowerCase();
        SharePointMediaConnector conn = basePathToConnectorMap.get(site);
        logger.trace("getOrCreateCachedConnectorForRootSite: Cached connector found for site={}, siteAddendum={}, conn={}", site, siteAddendum, conn);
        if (conn == null) {
            synchronized (basePathToConnectorMap) {
                conn = basePathToConnectorMap.get(site);
                if (conn == null) {
                    logger.trace("getOrCreateCachedConnectorForRootSite: Cached connector on found for site={} (siteAddendum={}). Creating and adding to cache", site, siteAddendum);
                    conn = StringUtils.EMPTY.equals(siteAddendum) ? this : recreateConnectorWithAdjustedParams(siteAddendum);
                    basePathToConnectorMap.put(site, conn);
                }
            }
        }

        return conn;*/

        return recreateConnectorWithAdjustedParams(siteAddendum);
    }

    private void postScan() {
        String longestFstLvlKey = fstLvlFoldersToConnectorMap.keySet()
                .stream()
                .max(Comparator.comparingInt(String::length))
                .orElse("No keys");

        String logenstBasePathToConnectorKey = basePathToConnectorMap.keySet()
                .stream()
                .max(Comparator.comparingInt(String::length))
                .orElse("No keys");
        logger.info("Post scan stats: fstLvlFoldersToConnectorMap size {} (longest key={}, length={}), " +
                        "fstLvlFoldersToConnectorMap size {} (longest key={}, length={}), ",
                fstLvlFoldersToConnectorMap.size(), longestFstLvlKey, longestFstLvlKey.length(),
                basePathToConnectorMap.size(), logenstBasePathToConnectorKey, logenstBasePathToConnectorKey.length());

        closedResourceStaleConnections();
    }

    protected List<ServerResourceDto> listSubSites(MSItemKey key, String basePath) {
        String siteStr = SharePointParseUtils.encodeSubSiteNameIfNeeded(key.getSite());

        String siteWithBase = SharePointParseUtils.normalizePath(basePath) +
                Optional.ofNullable(siteStr)
                        .map(SharePointParseUtils::normalizePath)
                        .orElse(StringUtils.EMPTY);

        final boolean isEmptyBasePath = isEmptyPath(basePath);
        try {
            List<ServerResourceDto> subSites = execAsyncTask(() -> microsoftDocAuthorityClient.listSubSitesUnderSubSite(SharePointParseUtils.normalizePath(siteWithBase)));
            subSites.stream()
                    .peek(dto -> {
                        String subSiteTmp;
                        if (isEmptyBasePath) {
                            subSiteTmp = dto.getFullName().substring(this.createBaseUri(true).length());
                        } else {
                            subSiteTmp = dto.getFullName().substring(dto.getFullName().indexOf(basePath) + basePath.length());
                        }
                        String address = SharePointParseUtils.removeUnneededDoubleSlashes(dto.getFullName());
                        dto.setFullName(SharePointParseUtils.applySiteMark(address, subSiteTmp));
                    })
                    .map(ServerResourceDto::getFullName)
                    .map(fullName -> {
                        fullName = SharePointParseUtils.splitPathAndSubsite(fullName).getPath();
                        if (fullName.startsWith("/")) {
                            fullName = fullName.substring(1);
                        }
                        return fullName;
                    })
                    .forEach(detectedSubSites::add);
            return subSites;
        } catch (Exception e) {
            String path = SharePointParseUtils.applySiteMark(key.getPath(), key.getSite());
            logger.error("Failed to list sub-sites under " + path, e);
            return Lists.newArrayList();
        }
    }


    @Override
    public String toString() {
        return "SharePointMediaConnector{" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", userName='" + userName + '\'' +
                ", domain='" + domain + '\'' +
                '}';
    }


    public static SharePointMediaConnectorBuilder builder() {
        return new SharePointMediaConnectorBuilder();
    }

    public static class SharePointMediaConnectorBuilder extends MicrosoftConnectorBaseBuilder<SharePointMediaConnector, SharePointMediaConnectorBuilder> {

        private int siteCrawlMaxDepth = -1;

        private int scanIterationTimes = -1;

        public SharePointMediaConnectorBuilder withSiteCrawlMaxDepth(int siteCrawlMaxDepth) {
            this.siteCrawlMaxDepth = siteCrawlMaxDepth;
            return getThis();
        }

        public SharePointMediaConnectorBuilder withScanIterationTimes(int timesToScan) {
            this.scanIterationTimes = timesToScan;
            return getThis();
        }

        @Override
        protected SharePointMediaConnectorBuilder getThis() {
            return this;
        }

        @Override
        public SharePointMediaConnector build() {
            return scanIterationTimes == -1
                    ? new SharePointMediaConnector(
                    sharePointConnectionParametersDto,
                    appInfo,
                    maxRetries,
                    pageSize,
                    maxFileSize,
                    connectionConfig,
                    siteCrawlMaxDepth,
                    folderToFail,
                    foldersToFilter,
                    isSpecialCharsSupported,
                    maxPathCrawlingDepth,
                    maxIdenticalNameInPath,
                    pathMismatchSkip,
                    charsToFilter)
                    : new SharePointMediaConnectorMultiple(sharePointConnectionParametersDto,
                    appInfo,
                    maxRetries,
                    pageSize,
                    maxFileSize,
                    connectionConfig,
                    siteCrawlMaxDepth,
                    folderToFail,
                    scanIterationTimes,
                    foldersToFilter,
                    isSpecialCharsSupported,
                    maxPathCrawlingDepth,
                    maxIdenticalNameInPath,
                    pathMismatchSkip,
                    charsToFilter);
        }
    }

    protected List<ServerResourceDto> testConnectionImpl() {
        return browseSiteFolders(basePath);
    }
}
// ########################################## getLibraryListObject ##################################################

//    private com.middleware.share.List getLibraryListObject(String library) throws FileNotFoundException {
//        String internalName = SharePointParseUtils.encodeToInternalName(library);
//        logger.debug("Get library {} (internal: {}) list details", library, internalName);
//        com.middleware.share.List list;
//        List<IQueryOption> queryOptions = new ArrayList<>();
//        IFilterRestriction filterRestriction = new IsEqualTo("entitytypename", internalName);
//        queryOptions.add(new Filter(filterRestriction));
//        try {
//            List<com.middleware.share.List> lists = service.getLists(queryOptions);
//            if (lists.size() == 0) {
//                logger.warn("Library {} not found under internal name {}", library, internalName);
//                throw new FileNotFoundException(library);
//            }
//            list = lists.get(0);
//        } catch (ServiceException e) {
//            logger.error("Failed to get Lists from SharePoint: " + createBaseUri(true), e);
//            if (e.getCause() != null && e.getCause() instanceof UnknownHostException) {
//                throw new FileNotFoundException("Unknown host " + host + " (" + e.getMessage() + ")");
//            }
//            throw new RuntimeException("Failed to get lists from sharePoint (" + e.getMessage() + ")", e);
//        }
//        return list;
//    }

// ###########################################################################################

//    @Override
//    public Stream<MediaFolderWithFiles> streamFoldersAndFiles(String baseFolderId,
//               List<FolderExcludeRuleDto> nonEqualExcludedRules, List<Path> pathList2Skip,
//               List<String> dirnameList2Skip, Long runId) {
//        return innerStreamFoldersAndFiles(baseFolderId, nonEqualExcludedRules, pathList2Skip, dirnameList2Skip, runId);
//    }

//    private Stream<MediaFolderWithFiles> innerStreamFoldersAndFiles(String path, List<FolderExcludeRuleDto> nonEqualExcludedRules,
//                                                                    List<Path> pathList2Skip, List<String> dirnameList2Skip,
//                                                                    Long runId) {
//        //TO-DO - check if we need to skip the folder
//        //TO-DO - check scan cap
//        try {
//            //Return the folders with files
//            SharePointMediaFolderWithFiles folderWithFiles = new SharePointMediaFolderWithFiles(path);
//            List<File> files = listFiles(path);
//            folderWithFiles.setFiles(files);
//            Stream<MediaFolderWithFiles> pathStream = listFolders(path).stream()
//                    .flatMap(f -> innerStreamFoldersAndFiles(f.getFullName(), nonEqualExcludedRules, pathList2Skip, dirnameList2Skip, runId));
//            return Stream.concat(Stream.of(folderWithFiles), pathStream);
//
//        }
//        catch (ServiceException e) {
//            logger.warn("Service Exception while listing items in folder {}. {}", path, e);
//            scanErrorsService.addError("Service Exception Error while listing items in folder", e, null, path, runId);
//        }
//        catch (Exception e) {
//            logger.error("Unexpected error while listing items in folder {}. {}", path, e);
//            scanErrorsService.addError("System error while listing items in folder", e, null, path, runId);
//        }
//        //TO-DO - create folder in ERROR state
//        return Stream.empty();
//    }


//    private Folder getFolderData(String path) {
//        try {
//            Folder folder = service.getFolder(path);
//            logger.debug("Folder path {} has name {}", path, folder.getName());
//            return folder;
//
//        } catch (ServiceException e) {
//            logger.error("Failed to get folder {} data", path, e);
//            throw new RuntimeException("Failed to get folder {} data", e);
//        }
//    }

//    public RawDocumentMessageDto getFileData(String path) {
//        try {
//            File file = service.getFile(path);
//            List<Field> listFields = service.getListFields(file.getUniqueId());
//            for (Field listField : listFields) {
//                logger.debug(listField.toString());
//            }
//
//            byte[] fileContent = service.getFileContent(path);
//            RawDocumentMessageDto rawDocumentMessageDto = new RawDocumentMessageDto(path, fileContent, null, null, null);
//            return rawDocumentMessageDto;
//        } catch (ServiceException e) {
//            logger.error("Failed to fetch file content for {}", path, e);
//            throw new RuntimeException("Failed to fetch file content for " + path, e);
//        }
//    }

//    public List<ClaFilePropertiesDto> listItems(String listId, int count, String lastMediaItemId) {
//        //http://ec2-54-200-41-63.us-west-2.compute.amazonaws.com/sites/test/_api/web/lists('f1f04276-593b-454b-8ee1-006f83af18d3')/Items?$top=5&$expand=FieldValuesAsText
//        try {
//            if (lastMediaItemId != null) {
//                lastMediaItemId = StringUtils.split(lastMediaItemId, "/")[1];
//                logger.debug("List {} items from {}", count, lastMediaItemId);
//            } else {
//                logger.debug("List {} items from the beginning", count);
//            }
//            List<IQueryOption> queryOptions = new ArrayList<>();
//            queryOptions.add(new SharepointPagingToken(lastMediaItemId, count));
//            queryOptions.add(new Expand("File/Author,FieldValuesAsText"));
//            queryOptions.add(new Select("*", "HasUniqueRoleAssignments"));
//            SharePointListItemPage listItems = getListItems(listId, queryOptions);
//            logger.debug("Next page: {}",listItems.getNextUrl());
//            return convertSharePointListItemsToFiles(listId, listItems.getItems());
////            List<ListItem> listItems = service.getListItems(listId, queryOptions);
////            return convertListItemsToFiles(listId, listItems);
//        } catch (ServiceException e) {
//            logger.error("Failed to list items from list {}", listId, e);
//            throw new RuntimeException("Failed to list items from list " + listId, e);
//        }
//    }

//    private com.middleware.share.List getLibraryListObjectByPath(String path) throws FileNotFoundException {
//        com.middleware.share.List list;
//        List<IQueryOption> queryOptions = new ArrayList<>();
//        IFilterRestriction filterRestriction = new StartsWith("DocumentTemplateUrl", path);
//        queryOptions.add(new Filter(filterRestriction));
//        try {
//            List<com.middleware.share.List> lists = service.getLists(queryOptions);
//            if (lists.size() == 0) {
//                logger.warn("Library not found under path {}", path);
//                throw new FileNotFoundException(path);
//            }
//            list = lists.get(0);
//        } catch (ServiceException e) {
//            logger.error("Failed to get Lists from SharePoint: " + createBaseUri(), e);
//            if (e.getCause() != null && e.getCause() instanceof UnknownHostException) {
//                throw new FileNotFoundException("Unknown host " + host + " (" + e.getMessage() + ")");
//            }
//            throw new RuntimeException("Failed to get lists from sharePoint (" + e.getMessage() + ")", e);
//        }
//        return list;
//    }

//    private static AuthenticationResult getAccessTokenFromUserCredentials(String resource,
//                                                                          String username, String password) {
//        AuthenticationContext context;
//        AuthenticationResult result = null;
//        ExecutorService service = null;
//        try {
//            service = Executors.newFixedThreadPool(1);
//            context = new AuthenticationContext(AUTHORITY_URL, false, service);
//            Future<AuthenticationResult> future = context.acquireToken(
//                    resource, CLIENT_ID, username, password,
//                    null);
//            result = future.get();
//        } catch (Exception e) {
//            logger.error("Failed to connect to sharePoint at {}", resource, e);
//            throw new RuntimeException("Failed to connect to sharePoint at " + resource, e);
//        } finally {
//            service.shutdown();
//        }
//
//        if (result == null) {
//            throw new RuntimeException(
//                    "authentication result was null");
//        }
//        return result;
//    }

//    private int sharePointType = 3;
//    private List<String> cookieStore;
//    private ContextInfo contextInfo;
//
//    private InputStream getInputStream(String method, String requestUrl, String body, String xHttpMethod, String ifMatchHeader,  boolean realAllInputStream) throws Exception {
//        System.setProperty("http.auth.preference", "basic");
//        System.setProperty("sun.security.ssl.allowUnsafeRenegotiation", "true");
//        String fullUrl;
//        String baseUrl;
//        String bodyText;
//        if (this.sharePointType == 3) {
//            URL urlObject;
//            if (this.userName != null && this.userName.endsWith(".onmicrosoft.com")) {
//                this.sharePointType = 1;
//            } else if (this.url != null && this.url.length() > 0) {
//                if ((urlObject = new URL(this.url)).getHost() == null || !urlObject.getHost().toLowerCase().endsWith(".sharepoint.com") && !urlObject.getHost().toLowerCase().endsWith(".sharepointonline.com") && !urlObject.getHost().toLowerCase().endsWith(".office365.com")) {
//                    this.sharePointType = 2;
//                } else {
//                    this.sharePointType = 1;
//                }
//            } else {
//                this.sharePointType = 2;
//            }
//
//            if (this.sharePointType == 1) {
//                urlObject = new URL(this.url);
//                baseUrl = urlObject.getProtocol() + "://" + urlObject.getHost();
//                bodyText = "<S:Envelope xmlns:S=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:wsa=\"http://www.w3.org/2005/08/addressing\" xmlns:wsp=\"http://schemas.xmlsoap.org/ws/2004/09/policy\" xmlns:wsse=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\" xmlns:wst=\"http://schemas.xmlsoap.org/ws/2005/02/trust\" xmlns:wsu=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd\"><S:Header><wsa:Action S:mustUnderstand=\"1\">http://schemas.xmlsoap.org/ws/2005/02/trust/RST/Issue</wsa:Action><wsa:To S:mustUnderstand=\"1\">https://login.microsoftonline.com/rst2.srf</wsa:To><wsse:Security><wsse:UsernameToken wsu:Id=\"user\"><wsse:Username>" + this.userName + "</wsse:Username><wsse:Password>" + this.password + "</wsse:Password></wsse:UsernameToken>" + "</wsse:Security></S:Header>" + "<S:Body><wst:RequestSecurityToken Id=\"RST0\">" + "<wst:RequestType>http://schemas.xmlsoap.org/ws/2005/02/trust/Issue</wst:RequestType><wsp:AppliesTo>" + "<wsa:EndpointReference><wsa:Address>" + baseUrl + "</wsa:Address></wsa:EndpointReference>" + "</wsp:AppliesTo><wsp:PolicyReference URI=\"MBI\" />" + "</wst:RequestSecurityToken></S:Body>" + "</S:Envelope>";
////                if ((fullUrl = extractConnectionElement(this.getInputStream("https://login.microsoftonline.com/rst2.srf", bodyText))) != null) {
////                    baseUrl = baseUrl.endsWith("/") ? baseUrl + "_forms/default.aspx?wa=wsignin1.0" : baseUrl + "/_forms/default.aspx?wa=wsignin1.0";
////                    this.getInputStream(baseUrl, fullUrl);
////                }
//            }
//        }
//
//        fullUrl = this.url.endsWith("/") ? this.url + requestUrl : this.url + "/" + requestUrl;
//        CloseableHttpResponse httpResponse = null;
//        boolean retry = true;
//
//        while (true) {
//            HttpRequestBase httpRequest;
//            while (retry) {
//                URI requestUri;
//                URI uri = requestUri = new URI(fullUrl);
//                if (method.equals("GET")) {
//                    httpRequest = new HttpGet(uri);
//                } else if (method.equals("DELETE")) {
//                    httpRequest = new HttpDelete(uri);
//                } else if (method.equals("PUT")) {
//                    httpRequest = new HttpPut(uri);
//                } else {
//                    httpRequest = new HttpPost(uri);
//                    if (body != null) {
//                        StringEntity bodyEntity;
//                        (bodyEntity = new StringEntity(body, "UTF-8")).setContentType("application/json;odata=verbose");
//                        ((HttpPost) httpRequest).setEntity(bodyEntity);
//                    }
//                }
//
////                httpRequest.setHeader("User-Agent", b.a);
//                httpRequest.setHeader("Accept", "application/atom+xml");
//                httpRequest.setHeader("Content-Type", "application/json;odata=verbose");
//                httpRequest.setHeader("Accept-Encoding", "gzip");
//
////                if (this.t != null) {
////                    ((HttpRequestBase) httpRequest).setHeaders(this.t);
////                }
//
//                if (xHttpMethod != null && xHttpMethod.length() > 0) {
//                    httpRequest.setHeader("X-HTTP-Method", xHttpMethod);
//                }
//
//                if (ifMatchHeader != null && ifMatchHeader.length() > 0) {
//                    httpRequest.setHeader("IF-MATCH", ifMatchHeader);
//                }
//
//                HttpClientBuilder httpClientBuilder = HttpClients.custom();
//                BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
//                if (this.sharePointType == 2) {
//                    basicCredentialsProvider.setCredentials(new AuthScope(requestUri.getHost(), requestUri.getPort(), AuthScope.ANY_REALM), new UsernamePasswordCredentials(this.userName, this.password));
//                    basicCredentialsProvider.setCredentials(new AuthScope(requestUri.getHost(), requestUri.getPort(), AuthScope.ANY_REALM), new NTCredentials(this.userName, this.password, requestUri.getHost(), this.domain != null ? this.domain : ""));
////                } else if (this.f != null) {
////                    httpClientBuilder.setDefaultCookieStore(this.f);
//                }
//
////                if (this.p != null && this.o != null) {
////                    basicCredentialsProvider.setCredentials(new AuthScope(this.p.getHostName(), this.p.getPort(), AuthScope.ANY_REALM), this.o);
////                }
//
//                Calendar calendar;
//                (calendar = Calendar.getInstance()).add(13, -60);
////                if ((this.contextInfo == null || this.contextInfo.getFormDigestValueExpireTime().compareTo(calendar.getTime()) < 0)) {
////                    this.contextInfo = this.getContextInfo();
////                }
//
//                if (this.contextInfo != null) {
//                    httpRequest.setHeader("X-RequestDigest", this.contextInfo.getFormDigestValue());
//                }
//
////                if (this.r != null) {
////                    httpClientBuilder.setDefaultRequestConfig(this.r);
////                }
//
////                if (this.q != null) {
////                    httpClientBuilder.setConnectionManager(this.q);
////                }
//
//                Registry objectRegistry = RegistryBuilder.create().register("NTLM", new NTLMSchemeFactory()).register("Basic", new BasicSchemeFactory()).register("Digest", new DigestSchemeFactory()).register("negotiate", new SPNegoSchemeFactory()).register("Kerberos", new KerberosSchemeFactory()).build();
//                httpClientBuilder.setDefaultAuthSchemeRegistry(objectRegistry);
//                this.httpclient = httpClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider).build();
//                StatusLine statusLine;
//                Header[] locationHeaders;
//                if ((statusLine = (httpResponse = this.httpclient.execute((HttpUriRequest) httpRequest)).getStatusLine()).getStatusCode() >= 300 && statusLine.getStatusCode() < 400) {
//                    if ((locationHeaders = httpResponse.getHeaders("Location")).length > 0) {
//                        fullUrl = locationHeaders[0].getValue();
//                        continue;
//                    }
//                } else if (statusLine.getStatusCode() == 500) {
//                    if ((locationHeaders = httpResponse.getHeaders("Content-Type")) != null && locationHeaders.length > 0 && locationHeaders[0].getValue() != null) {
//                        InputStream errorBody = httpResponse.getEntity().getContent();
//                        Header[] var27;
//                        if ((var27 = httpResponse.getHeaders("Content-Encoding")) != null && var27.length > 0 && (xHttpMethod = var27[0].getValue()) != null && xHttpMethod.equals("gzip")) {
//                            errorBody = new GZIPInputStream(new BufferedInputStream((InputStream) errorBody));
//                        }
//
//                        throw new RuntimeException("Service exception"+requestUrl+" "+body);
//                    }
//                } else if (statusLine.getStatusCode() >= 400) {
//                    throw new ServiceException(Integer.toString(statusLine.getStatusCode()) + " " + statusLine.getReasonPhrase(), (Throwable) null, requestUrl, body);
//                }
//
//                retry = false;
//            }
//
//            HttpEntity httpEntity;
//            if ((httpEntity = httpResponse.getEntity()) != null) {
//                InputStream result = httpEntity.getContent();
//                Header[] headers;
//                if ((headers = httpResponse.getHeaders("Content-Encoding")) != null && headers.length > 0 && (bodyText = headers[0].getValue()) != null && bodyText.equals("gzip")) {
//                    result = new GZIPInputStream(new BufferedInputStream((InputStream) result));
//                }
//
//                if (realAllInputStream) {
//                    result = readAllInputStream(result);
//                }
//
//                return (InputStream) result;
//            }
//
//            return null;
//        }
//    }
//
//    private static String extractConnectionElement(InputStream inputStream) throws Exception {
//        String var1 = null;
//        XMLStreamReader var3 = XMLInputFactory.newInstance().createXMLStreamReader(inputStream);
//
//        while (var3.hasNext() && var3.next() > 0) {
//            if (var3.isStartElement() && var3.getLocalName() != null && var3.getNamespaceURI() != null && var3.getLocalName().equals("BinarySecurityToken") && var3.getNamespaceURI().equals("http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd")) {
//                var1 = var3.getElementText();
//            }
//        }
//
//        return var1;
//    }
//
////    private ContextInfo getContextInfo() throws ServiceException {
////        InputStream inputStream = null;
////        String var2 = "_api/contextinfo";
////
////        try {
////            inputStream = this.getInputStream("POST", var2, (String)null, (String)null, (String)null, (InputStream)null, true, true);
////            this.contextInfo = new ContextInfo(inputStream);
////        } catch (ServiceException var13) {
////            throw var13;
////        } catch (Exception var14) {
////            throw new ServiceException(var14.getMessage(), var14, var2);
////        } finally {
////            if(inputStream != null) {
////                try {
////                    inputStream.close();
////                } catch (IOException var12) {
////                    throw new ServiceException(var12.getMessage(), var12, var2);
////                }
////            }
////
////            if(this.httpclient != null) {
////                try {
////                    this.httpclient.close();
////                } catch (IOException var11) {
////                    throw new ServiceException(var11.getMessage(), var11, var2);
////                }
////            }
////
////        }
////
////        return this.contextInfo;
////    }
//
//    private static InputStream readAllInputStream(InputStream var0) throws IOException {
//        BufferedInputStream bufferedInputStream = new BufferedInputStream(var0);
//        ByteArrayOutputStream var1 = new ByteArrayOutputStream();
//        byte[] var2 = new byte[2048];
//
//        int var3;
//        try {
//            var3 = bufferedInputStream.read(var2);
//
//            while(true) {
//                if(var3 == -1) {
//                    var2 = var1.toByteArray();
//                    break;
//                }
//
//                var1.write(var2, 0, var3);
//                var3 = bufferedInputStream.read(var2);
//            }
//        } finally {
//            bufferedInputStream.close();
//            var1.close();
//        }
//
//        for(var3 = 0; var3 < var2.length; ++var3) {
//            if(var2[var3] == 25) {
//                var2[var3] = 32;
//            } else if(var2[var3] == 38 && var3 < var2.length - 3 && var2[var3 + 1] == 35 && var2[var3 + 2] == 120 && var2[var3 + 3] != 9 && var2[var3 + 3] != 65 && var2[var3 + 3] != 68) {
//                var2[var3] = 32;
//            }
//        }
//
//        return new ByteArrayInputStream(var2);
//    }

