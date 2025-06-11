package com.microsoft.sharepoint;

import com.file.ClaFilePropertiesDto;
import com.file.ServerResourceDto;
import com.media.MediaChangeLogDto;
import com.StreamMediaItemsParams;
import com.microsoft.MSItemKey;
import com.microsoft.model.*;
import com.middleware.share.File;
import com.middleware.share.Folder;
import com.middleware.share.ServiceException;
import com.middleware.share.User;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)


public class SharePointMediaConnectorMultipleTests extends SharepointConnectorTests {
    private static final Logger logger = LoggerFactory.getLogger(SharePointMediaConnectorMultipleTests.class);

    private SharePointMediaConnectorMultiple sharePointMediaConnectorMultiple;

    private static final int timesToRescan = 10;

    @Before
    public void init() {
        sharePointMediaConnectorMultiple = (SharePointMediaConnectorMultiple) getEnrichedConnector(SharePointMediaConnector.builder().withScanIterationTimes(timesToRescan), SITE_URL);
        initConnectorResources(sharePointMediaConnectorMultiple);
    }

    @Test
    public void test_streamMediaItems_documentLibraryWithFilesOnly() throws ServiceException {
        String docLibPath = "/doc-lib";
        MSFolder folder = buildFolderWithItems(docLibPath, 0);
        MSDocumentLibrary docLib = buildDocumentLibrary(Optional.of(folder.getFiles()), 0, Optional.empty());
        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();

        SharePointExtendedFolder sharePointExtendedFolder = docLib.toSharePointExtendedFolder();
        assertFalse(sharePointExtendedFolder.equals(null));

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(docLibPath))).thenReturn(sharePointExtendedFolder);
        when(microsoftDocAuthorityClient.getFilesWithMediaItemId(anyString(), eq(docLibPath), eq(docLib.getListId()), anyInt(), anyInt())).thenReturn(docLib.getFilesAsClaProp());

        String pathToScan = SITE_URL + docLibPath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true);

        sharePointMediaConnectorMultiple.streamMediaItems(params);

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

        assertEquals("Not all items were detected", (docLib.getFiles().size() + 1)*timesToRescan, accumulator.size()); // +1 for  the document library itself
        assertTrue("Document library item wasn't consumed", foldersCrawled.contains(docLibPath.substring(1)));
    }

    @Test
    public void test_streamMediaItems_documentLibrary() throws ServiceException {
        String docLibPath = "/doc-lib";
        MSFolder folder = buildFolderWithItems(docLibPath, 0);
        MSDocumentLibrary docLib = buildDocumentLibrary(Optional.of(folder.getFiles()), 4, Optional.empty());
        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();

        SharePointExtendedFolder sharePointExtendedFolder = docLib.toSharePointExtendedFolder();
        assertFalse(sharePointExtendedFolder.equals(null));

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(docLibPath))).thenReturn(sharePointExtendedFolder);
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

        sharePointMediaConnectorMultiple.streamMediaItems(params);

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

        assertEquals("Not all items were detected: ", itemCount*timesToRescan, accumulator.size()); // +1 for the doc-lib itself
        assertTrue("Document library item wasn't consumed", foldersCrawled.contains(docLibPath.substring(1)));
    }

    @Test
    public void test_streamMediaItems_site() throws ServiceException {
        String sitePath = "";
        MSSite site = buildSharepointSite(sitePath, 2);

        SharePointExtendedFolder sharePointExtendedFolder = site.toSharePointExtendedFolder();
        assertFalse(sharePointExtendedFolder.equals(null));

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(sitePath))).thenReturn(sharePointExtendedFolder);

        wireSiteMocks(site, sitePath);

        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();
        String pathToScan = SITE_URL + sitePath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true);

        sharePointMediaConnectorMultiple.streamMediaItems(params);

        verify(microsoftDocAuthorityClient, times(3*timesToRescan)).listSubSitesUnderSubSite(anyString());
    }

    @Test
    public void test_concurrentStreamMediaItems_documentLibraryWithFilesOnly() throws ServiceException {
        String docLibPath = "/doc-lib";
        MSFolder folder = buildFolderWithItems(docLibPath, 0);
        MSDocumentLibrary docLib = buildDocumentLibrary(Optional.of(folder.getFiles()), 0, Optional.empty());
        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();

        SharePointExtendedFolder sharePointExtendedFolder = docLib.toSharePointExtendedFolder();
        assertFalse(sharePointExtendedFolder.equals(null));

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(docLibPath))).thenReturn(sharePointExtendedFolder);
        when(microsoftDocAuthorityClient.getFilesWithMediaItemId(anyString(), eq(docLibPath), eq(docLib.getListId()), anyInt(), anyInt())).thenReturn(docLib.getFilesAsClaProp());

        String pathToScan = SITE_URL + docLibPath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true)
                .setForkJoinPool(new ForkJoinPool());

        sharePointMediaConnectorMultiple.concurrentStreamMediaItems(params);

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

        assertEquals("Not all items were detected", (docLib.getFiles().size() + 1)*timesToRescan, accumulator.size()); // +1 for  the document library itself
        assertTrue("Document library item wasn't consumed", foldersCrawled.contains(docLibPath.substring(1)));
    }

    @Test
    public void test_concurrentStreamMediaItems_site() throws ServiceException {
        String sitePath = "";
        MSSite site = buildSharepointSite(sitePath, 2);

        SharePointExtendedFolder sharePointExtendedFolder = site.toSharePointExtendedFolder();
        assertFalse(sharePointExtendedFolder.equals(null));

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(sitePath))).thenReturn(sharePointExtendedFolder);

        wireSiteMocks(site, sitePath);

        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();
        String pathToScan = SITE_URL + sitePath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true)
                .setForkJoinPool(new ForkJoinPool());

        sharePointMediaConnectorMultiple.concurrentStreamMediaItems(params);

        verify(microsoftDocAuthorityClient, times(3*timesToRescan)).listSubSitesUnderSubSite(anyString());
    }

    @Test
    public void test_changeSubSiteParams() throws ServiceException {
        String docLibPath = "/doc-lib";
        MSFolder folder = buildFolderWithItems(docLibPath, 0);
        MSDocumentLibrary docLib = buildDocumentLibrary(Optional.of(folder.getFiles()), 0, Optional.empty());
        List<ClaFilePropertiesDto> accumulator = Lists.newArrayList();

        SharePointExtendedFolder sharePointExtendedFolder = docLib.toSharePointExtendedFolder();
        assertFalse(sharePointExtendedFolder.equals(null));

        when(microsoftDocAuthorityClient.getSharePointExtendedFolderDetails(anyString(), eq(docLibPath))).thenReturn(sharePointExtendedFolder);
        when(microsoftDocAuthorityClient.getFilesWithMediaItemId(anyString(), eq(docLibPath), eq(docLib.getListId()), anyInt(), anyInt())).thenReturn(docLib.getFilesAsClaProp());

        String pathToScan = SITE_URL + docLibPath;

        StreamMediaItemsParams params = StreamMediaItemsParams.create()
                .setScanParams(getScanTaskParams(pathToScan)).setFilePropertiesConsumer(accumulator::add)
                .setFilePropsProgressTracker(filePropsProgressTracker)
                .setScanActivePredicate(aLong -> true);

        sharePointMediaConnectorMultiple.streamMediaItems(params);

        String subSite = "/{subsite}";
        params.setRealPath(SITE_URL + subSite + docLibPath).setChangeConsumer(new Consumer<MediaChangeLogDto>() {
            @Override
            public void accept(MediaChangeLogDto mediaChangeLogDto) {

            }
            @Override
            public Consumer<MediaChangeLogDto> andThen(Consumer<? super MediaChangeLogDto> after) {
                return Consumer.super.andThen(after);
            }
        });

        boolean exceptionFlag = false;
        try {
            sharePointMediaConnectorMultiple.listPrincipals();

            sharePointMediaConnectorMultiple.streamMediaChangeLog(params);
            List<File> files = sharePointMediaConnectorMultiple.listFiles(subSite, pathToScan);
            List<ServerResourceDto> serverResourceDtos = sharePointMediaConnectorMultiple.listFilesAsServerResources(subSite, pathToScan);
            serverResourceDtos = sharePointMediaConnectorMultiple.listFolders(subSite, pathToScan);
            // ClaFilePropertiesDto claFilePropertiesDto = sharePointMediaConnectorMultiple.getFolderAttributes(subSite, pathToScan, false);

        }
        catch (Exception ex) {
            logger.info(sharePointMediaConnectorMultiple.toString());
            exceptionFlag = true;
        }
        assertFalse(exceptionFlag);
    }
}