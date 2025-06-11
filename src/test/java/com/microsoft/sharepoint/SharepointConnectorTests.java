package com.microsoft.sharepoint;

import com.file.ClaFilePropertiesDto;
import com.StreamMediaItemsParams;
import com.microsoft.MicrosoftTestBase;
import com.microsoft.model.*;
import com.middleware.share.Folder;
import com.middleware.share.ListBaseType;
import com.middleware.share.ServiceException;
import com.middleware.share.queryoptions.Filter;
import com.middleware.share.queryoptions.IFilterRestriction;
import com.middleware.share.queryoptions.IQueryOption;
import com.middleware.share.queryoptions.IsEqualTo;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SharepointConnectorTests extends MicrosoftTestBase {

    private static final Logger logger = LoggerFactory.getLogger(SharepointConnectorTests.class);

    protected static final String SITE_URL = "https://sharepoint.instance.com";

    private static final List<MSDocumentLibrary.DocumentLibraryProperties> PREDEFINED_DOC_LIBS = Arrays.asList(
            new MSDocumentLibrary.DocumentLibraryProperties("/Shared Documents/Forms/template.dotx", "Shared_x0020_Documents", "Documents", "/Shared Documents", true),
            new MSDocumentLibrary.DocumentLibraryProperties("/Lists/AppPackages/Forms/template.dotx", "AppPackagesList", "packList", "/bla bla", false),
            new MSDocumentLibrary.DocumentLibraryProperties("", "OData__x005f_catalogs_x002f_appfiles", "catalog files", "", false),
            new MSDocumentLibrary.DocumentLibraryProperties("/docauthorityLibrary/Forms/template.dotx", "DocauthorityLibrary", "Da Lib", "/docauthorityLibrary", true));

    private static final List<IQueryOption> queryOptions;

    private int predefinedDocLibIndex = 0;

    private SharePointMediaConnector sharePointMediaConnector;

    static {
        queryOptions = new ArrayList<>();
        IFilterRestriction filterRestriction = new IsEqualTo("baseType", ListBaseType.DOCUMENT_LIBRARY.ordinal());
        queryOptions.add(new Filter(filterRestriction));
    }

    @Before
    public void init()  {
        sharePointMediaConnector = getEnrichedConnector(SharePointMediaConnector.builder(), SITE_URL);
        initConnectorResources(sharePointMediaConnector);
    }

    protected String extractFilename(ClaFilePropertiesDto prop) {
        String fileName = prop.getFileName();
        return fileName.substring(fileName.lastIndexOf("/") + 1);
    }

    // Folder with sub folders + files
    protected MSFolder buildFolderWithItems(String path, int maxDepth) {
        MSFolder.Builder folderBuilder = MSFolder.Builder.create()
                .withPath(path)
                .withCreateTime(Date.from(Instant.now().minusMillis(100000)))
                .withLastModTime(Date.from(Instant.now()))
                .withFileAmount(66);

        if (maxDepth > 0) {
            IntStream.range(0, 4)
                    .mapToObj(index -> buildFolderWithItems(generateFolderName(maxDepth, index), maxDepth - 1))
                    .forEach(folderBuilder::withSubFolder);
        }

        return folderBuilder.build();
    }

    private String generateFolderName(int depth, @Nullable Integer index) {
        String idxAdd = Optional.ofNullable(index)
                .map(idx -> "." + index)
                .orElseGet(() -> {
                    String id = UUID.randomUUID().toString();
                    return "-" + id.substring(0, id.indexOf("/"));
                });

        return "/FOLDER-" + depth + idxAdd;
    }

    protected MSDocumentLibrary buildDocumentLibrary(Optional<List<MSFile>> docLibFiles, int folderDepth, Optional<String> sitePrefix) {
        MSDocumentLibrary.MSDocumentLibraryBuilder docLibBuilder = MSDocumentLibrary.builder();
        docLibFiles.ifPresent(docLibBuilder::files);
        if (folderDepth > 0) {
            IntStream.range(0, 6)
                    .mapToObj(index -> buildFolderWithItems(generateFolderName(folderDepth, index), folderDepth-1))
                    .forEach(docLibBuilder::folder);
        }

        docLibBuilder.listId(UUID.randomUUID().toString().toUpperCase());
        if (predefinedDocLibIndex >= PREDEFINED_DOC_LIBS.size()) {
            predefinedDocLibIndex = 0;
        }
        MSDocumentLibrary.DocumentLibraryProperties prop = PREDEFINED_DOC_LIBS.get(predefinedDocLibIndex++);
        if (!StringUtils.EMPTY.equals(prop.documentTemplateUrl)) {
            docLibBuilder.documentTemplateUrl(sitePrefix.orElse(StringUtils.EMPTY) + prop.documentTemplateUrl);
        }
        docLibBuilder.entityTypeName(prop.entityTypeName);
        docLibBuilder.title(prop.title);
        docLibBuilder.urlSegment(prop.urlSegment);
        docLibBuilder.crawlable(prop.isCrawlable);
        if (!prop.isCrawlable) {
            docLibBuilder.files(Lists.emptyList());
            docLibBuilder.folders(Lists.emptyList());
        }

        return docLibBuilder.build();
    }

    protected void wireMockedFolderCrawling(String parentPath, MSFileSystemItem msFolder, Optional<MSSite> siteOpt, String rootSitePath) {
        String subSite = siteOpt.map(MSSite::getSiteRelativeUrl)
                .map(url -> url.length() > 1 ? url.substring(1) : url)
                .map(url -> url.length() == 0 ? null : url)
                .orElse(null);
        String sitePath = siteOpt.map(MSSite::getSiteRelativeUrl).orElse(StringUtils.EMPTY);
        msFolder.getSubFolders().stream()
                .map(folder -> (MSFolder) folder)
                .peek(folder -> {
                    System.out.println("Mocked folder: " + sitePath + parentPath + folder.path + "|: " + folder.getFilesAsClaFileProp());
                    String path = sitePath + parentPath + folder.path;
                    try {
                        when(microsoftDocAuthorityClient.getFilesWithMediaItemId(anyString(), eq(sitePath + parentPath + folder.path), anyString(), anyInt(), anyInt())).thenReturn(folder.getFilesAsClaFileProp());
                        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(parentPath + folder.path))).thenReturn(folder.toSharePointExtendedFolder());
                        when(service.getFolders(subSite, path + "/")).thenReturn(folder.getFolders());
                        when(service.getFolders(subSite, path)).thenReturn(folder.getFolders());
                    } catch (ServiceException e) {
                        throw new RuntimeException(e);
                    }
                })
                .forEach(folder -> wireMockedFolderCrawling(SharePointParseUtils.normalizePath(parentPath + folder.path + "/"), folder, siteOpt, rootSitePath));
    }

    protected void wireSiteMocks(MSSite site, String rootSitePath) {
        String sitePath = SharePointParseUtils.normalizePath(site.getSiteRelativeUrl().substring(rootSitePath.length()));

        site.getDocLibs().forEach(lib -> wireDocumentLibraries(site, rootSitePath, lib));

        try {
            String siteWithBase;
            sitePath = sitePath.substring(1);
            if (StringUtils.EMPTY.equals(sitePath)) {
                siteWithBase = "/";
                sitePath = null;
            } else {
                siteWithBase = SharePointParseUtils.normalizePath(sitePath);
            }
            when(service.getLists(eq(sitePath), refEq(queryOptions))).thenReturn(getSiteDocLibJList(site));
            when(microsoftDocAuthorityClient.listSubSitesUnderSubSite(eq(siteWithBase))).thenReturn(site.getSubSitesAsServerResourceDto());
        } catch (ServiceException e) {
            logger.error("Failure", e);
        }
        site.getSubSites().forEach(subsite -> wireSiteMocks(subsite, rootSitePath));
    }

    private void wireDocumentLibraries(MSSite site, String rootSitePath, MSDocumentLibrary lib) {
        List<Folder> folders = lib.getSubFolders().stream()
                .map(sfolder -> (Folder) sfolder)
                .collect(Collectors.toList());
        try {
            when(service.getFolders(anyString(), eq(site.getSiteRelativeUrl() + rootSitePath + lib.getUrlSegment() + "/"))).thenReturn(folders);
        } catch (ServiceException e) {
            logger.error("Failure", e);
        }

        try {
            String docLibPath = SharePointParseUtils.normalizePath(site.getSiteRelativeUrl() + "|" + lib.getUrlSegment());
            System.out.println("lib.getUrlSegment(): " + docLibPath + "|" + lib.getListId() + ": " + lib.getFilesAsClaProp());
            when(microsoftDocAuthorityClient.getFilesWithMediaItemId(anyString(),
                    eq(docLibPath),
                    eq(lib.getListId()),
                    anyInt(),
                    anyInt()))
                    .thenReturn(lib.getFilesAsClaProp());
        } catch (ServiceException e) {
            logger.error("Failure", e);
        }
        wireMockedFolderCrawling(rootSitePath + lib.getUrlSegment(), lib, Optional.of(site), rootSitePath);
    }

    protected int calcItemCount(MSFileSystemItem msItem) {
        if (!msItem.isCrawlable()) {
            return 0;
        }
        int count = 1; // current folder
        count += Optional.ofNullable(msItem.getFiles())
                .map(List::size)
                .orElse(0);

        if (msItem.getSubFolders() != null) {
            count +=
                    msItem.getSubFolders().stream()
                            .map(this::calcItemCount)
                            .reduce((prev, cur) -> prev + cur)
                            .orElse(0);
        }

        return count;
    }

    protected MSSite buildSharepointSite(String path, int maxSiteDepth) {
        if (maxSiteDepth == 0) {
            return null;
        }

        MSSite.MSSiteBuilder siteBuilder = MSSite.builder()
                .connectorPath(SITE_URL)
                .siteRelativeUrl(path);
        IntStream.range(0, 4)
                .mapToObj(i -> "DocLib-" + i)
                .map(libName -> buildDocumentLibrary(Optional.empty(), 3, Optional.ofNullable(path)))
                .peek(docLib -> {
                    try {
                        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(),
                                eq(path + docLib.getUrlSegment())))
                                .thenReturn(docLib.toSharePointExtendedFolder());
                    } catch (ServiceException e) {
                        logger.error("Failure", e);
                    }
                })
                .forEach(siteBuilder::docLib);

        IntStream.range(0, 2)
                .mapToObj(i -> path + "/ss" + i)
                .map(site -> buildSharepointSite(site, maxSiteDepth - 1))
                .filter(Objects::nonNull)
                .forEach(siteBuilder::subSite);

        return siteBuilder.build();
    }

    @Test
    public void test_streamMediaItems_documentLibraryWithFilesOnly() throws ServiceException {
        String docLibPath = "/doc-lib";
        MSFolder folder = buildFolderWithItems(docLibPath, 0);
        MSDocumentLibrary docLib = buildDocumentLibrary(Optional.of(folder.getFiles()), 0, Optional.empty());
        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(docLibPath))).thenReturn(docLib.toSharePointExtendedFolder());
        when(microsoftDocAuthorityClient.getFilesWithMediaItemId(anyString(), eq(docLibPath), eq(docLib.getListId()), anyInt(), anyInt())).thenReturn(docLib.getFilesAsClaProp());

        String pathToScan = SITE_URL + docLibPath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true);

        sharePointMediaConnector.streamMediaItems(params);

        List<String> foldersCrawled = Lists.newArrayList();
        List<String> filesCrawled = accumulator.stream()
                .peek(item -> {
                    if (item.isFolder()) {
                        foldersCrawled.add(extractFilename(item));
                    }
                })
                .filter(ClaFilePropertiesDto::isFile)
                .map(this::extractFilename)
                .collect(Collectors.toList());

        docLib.getFiles().stream()
                .map(MSFile::getName)
                .forEach(fname -> assertTrue("Couldn't find file " + fname + " in the crawled files", filesCrawled.contains(fname)));

        assertEquals("Not all items were detected", docLib.getFiles().size() + 1, accumulator.size()); // +1 for  the document library itself
        assertTrue("Document library item wasn't consumed", foldersCrawled.contains(docLibPath.substring(1)));
    }

    @Test
    public void test_streamMediaItems_documentLibrary() throws ServiceException {
        String docLibPath = "/doc-lib";
        MSFolder folder = buildFolderWithItems(docLibPath, 0);
        MSDocumentLibrary docLib = buildDocumentLibrary(Optional.of(folder.getFiles()), 4, Optional.empty());
        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(docLibPath))).thenReturn(docLib.toSharePointExtendedFolder());
        when(microsoftDocAuthorityClient.getFilesWithMediaItemId(anyString(), eq(docLibPath), eq(docLib.getListId()), anyInt(), anyInt())).thenReturn(docLib.getFilesAsClaProp());

        wireMockedFolderCrawling(docLibPath, docLib, Optional.empty(), null);
        List<Folder> docLibFolders = docLib.getFolders().stream()
                .collect(Collectors.toList());
        when(service.getFolders(null, docLibPath + "/")).thenReturn(docLibFolders);

        String pathToScan = SITE_URL + docLibPath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true);

        sharePointMediaConnector.streamMediaItems(params);

        List<String> foldersCrawled = Lists.newArrayList();
        List<String> filesCrawled = accumulator.stream()
                .peek(item -> {
                    if (item.isFolder()) {
                        foldersCrawled.add(extractFilename(item));
                    }
                })
                .filter(ClaFilePropertiesDto::isFile)
                .map(this::extractFilename)
                .collect(Collectors.toList());

        docLib.getFiles().stream()
                .map(MSFile::getName)
                .forEach(fname -> assertTrue("Couldn't find file " + fname + " in the crawled files", filesCrawled.contains(fname)));

        int itemCount = calcItemCount(docLib);

        assertEquals("Not all items were detected: ", itemCount, accumulator.size()); // +1 for the doc-lib itself
        assertTrue("Document library item wasn't consumed", foldersCrawled.contains(docLibPath.substring(1)));
    }

    @Test
    public void test_streamMediaItems_site() throws ServiceException {
        String sitePath = "";
        MSSite site = buildSharepointSite(sitePath, 2);

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(sitePath))).thenReturn(site.toSharePointExtendedFolder());

        wireSiteMocks(site, sitePath);

        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();
        String pathToScan = SITE_URL + sitePath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true);

        sharePointMediaConnector.streamMediaItems(params);

        verify(microsoftDocAuthorityClient, times(3)).listSubSitesUnderSubSite(anyString());
    }

    private List<com.middleware.share.List> getSiteDocLibJList(MSSite site) {
        return site.getDocLibs().stream()
                .map(item -> (com.middleware.share.List) item)
                .collect(Collectors.toList());
    }

    @Test
    public void test_concurrentStreamMediaItems_documentLibraryWithFilesOnly() throws ServiceException {
        String docLibPath = "/doc-lib";
        MSFolder folder = buildFolderWithItems(docLibPath, 0);
        MSDocumentLibrary docLib = buildDocumentLibrary(Optional.of(folder.getFiles()), 0, Optional.empty());
        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(docLibPath))).thenReturn(docLib.toSharePointExtendedFolder());
        when(microsoftDocAuthorityClient.getFilesWithMediaItemId(anyString(), eq(docLibPath), eq(docLib.getListId()), anyInt(), anyInt())).thenReturn(docLib.getFilesAsClaProp());

        String pathToScan = SITE_URL + docLibPath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true)
                .setForkJoinPool(new ForkJoinPool());

        sharePointMediaConnector.concurrentStreamMediaItems(params);

        List<String> foldersCrawled = Lists.newArrayList();
        List<String> filesCrawled = accumulator.stream()
                .peek(item -> {
                    if (item.isFolder()) {
                        foldersCrawled.add(extractFilename(item));
                    }
                })
                .filter(ClaFilePropertiesDto::isFile)
                .map(this::extractFilename)
                .collect(Collectors.toList());

        docLib.getFiles().stream()
                .map(MSFile::getName)
                .forEach(fname -> assertTrue("Couldn't find file " + fname + " in the crawled files", filesCrawled.contains(fname)));

        assertEquals("Not all items were detected", docLib.getFiles().size() + 1, accumulator.size()); // +1 for  the document library itself
        assertTrue("Document library item wasn't consumed", foldersCrawled.contains(docLibPath.substring(1)));
    }

    @Test
    public void test_concurrentStreamMediaItems_site() throws ServiceException {
        String sitePath = "";
        MSSite site = buildSharepointSite(sitePath, 2);

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(sitePath))).thenReturn(site.toSharePointExtendedFolder());

        wireSiteMocks(site, sitePath);

        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();
        String pathToScan = SITE_URL + sitePath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true)
                .setForkJoinPool(new ForkJoinPool());

        sharePointMediaConnector.concurrentStreamMediaItems(params);

        verify(microsoftDocAuthorityClient, times(3)).listSubSitesUnderSubSite(anyString());
    }
}
