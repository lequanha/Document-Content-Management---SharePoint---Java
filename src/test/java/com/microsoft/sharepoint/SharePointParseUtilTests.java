package com.microsoft.sharepoint;

import com.microsoft.MSItemKey;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class SharePointParseUtilTests {

    @Test
    public void test_splitMediaItemIdAndSite() {
        String site = "site";
        String listId = UUID.randomUUID().toString();
        String itemId = UUID.randomUUID().toString().replaceAll("-", StringUtils.EMPTY);
        MSItemKey key = SharePointParseUtils.splitMediaItemIdAndSite(listId + SharePointParseUtils.LIST_ITEM_ID_SEPARATOR + itemId + SharePointParseUtils.SITE_DELIMITER + site);

        assertEquals(site, key.getSite());
        assertEquals(listId, key.getListId());
        assertEquals(itemId, key.getItemId());
        assertNull(key.getBasePathAddendum());
    }

    @Test
    public void test_splitMediaItemIdAndSiteAndBasePathCompletion() {
        String site = "site";
        String listId = UUID.randomUUID().toString();
        String itemId = UUID.randomUUID().toString().replaceAll("-", StringUtils.EMPTY);
        String basePathCompletion = UUID.randomUUID().toString().replaceAll("-", "/");
        MSItemKey key = SharePointParseUtils.splitMediaItemIdAndSite(listId + SharePointParseUtils.LIST_ITEM_ID_SEPARATOR
                + itemId + SharePointParseUtils.SITE_DELIMITER + site + SharePointParseUtils.BASE_PATH_COMPLETION_DELIMITER
                + basePathCompletion);

        assertEquals(site, key.getSite());
        assertEquals(listId, key.getListId());
        assertEquals(itemId, key.getItemId());
        assertEquals(basePathCompletion, key.getBasePathAddendum());
    }

    @Test
    public void test_splitMediaItemIdNoSiteAndBasePathCompletion() {
        String listId = UUID.randomUUID().toString();
        String itemId = UUID.randomUUID().toString().replaceAll("-", StringUtils.EMPTY);
        String basePathCompletion = UUID.randomUUID().toString().replaceAll("-", "/");
        MSItemKey key = SharePointParseUtils.splitMediaItemIdAndSite(
                listId + SharePointParseUtils.LIST_ITEM_ID_SEPARATOR + itemId
                + SharePointParseUtils.BASE_PATH_COMPLETION_DELIMITER + basePathCompletion);

        assertNull(key.getSite());
        assertEquals(listId, key.getListId());
        assertEquals(itemId, key.getItemId());
        assertEquals(basePathCompletion, key.getBasePathAddendum());
    }

    @Test
    public void test_splitPathAndSite() {
        String pathWithSubSite = "http://sp.ms.com/man-path/site/{subsite1}/{subsite2}";
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(pathWithSubSite);
        assertEquals("subsite1/subsite2", key.getSite());
        assertEquals("http://sp.ms.com/man-path/site/subsite1/subsite2", key.getPath());
    }

    @Test
    public void test_splitPathAndSiteAndFolder() {
        String pathWithSubSite = "http://sp.ms.com/man-path/site/{subsite1}/{subsite2}/docLib/folder";
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(pathWithSubSite);
        assertEquals("subsite1/subsite2", key.getSite());
        assertEquals("http://sp.ms.com/man-path/site/subsite1/subsite2/docLib/folder", key.getPath());
    }

    @Test
    public void test_splitPathAndSiteWithFile() {
        String pathWithSubSite = "http://sp.ms.com/man-path/site/{subsite1}/{subsite2}/docLib/folder/file.ofc";
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(pathWithSubSite);
        assertEquals("subsite1/subsite2", key.getSite());
        assertEquals("http://sp.ms.com/man-path/site/subsite1/subsite2/docLib/folder/file.ofc", key.getPath());
    }

    @Test
    public void test_splitPathNoSiteWithFile() {
        String pathWithSubSite = "http://sp.ms.com/man-path/site/docLib/folder/file.ofc";
        MSItemKey key = SharePointParseUtils.splitPathAndSubsite(pathWithSubSite);
        assertNull(key.getSite());
        assertEquals("http://sp.ms.com/man-path/site/docLib/folder/file.ofc", key.getPath());
    }

    @Test
    @Ignore
    public void test_applySiteMark() {
        throw new NotImplementedException("Impl me");
    }

    @Test
    public void test_applyBasePathCompletionNoSubSite() {
        String basePath = "mpath/site";
        String mediaItemId = "list-id/itemID";
        String newMediaItemId = SharePointParseUtils.applyBasePathCompletionToMediaItemId(basePath, mediaItemId);
        String expectedId = mediaItemId + SharePointParseUtils.BASE_PATH_COMPLETION_DELIMITER + basePath.toLowerCase();
        assertEquals(expectedId, newMediaItemId);

        basePath = "/" + basePath;
        newMediaItemId = SharePointParseUtils.applyBasePathCompletionToMediaItemId(basePath, mediaItemId);
        assertEquals(expectedId, newMediaItemId);
    }

    @Test
    public void test_applyBasePathCompletionWithSubSite() {
        String basePathCompletion = "site-completion";
        String subSite = "a-sub-site";
        String mediaItemId = "list-id/itemID";
        String newMediaItemId = SharePointParseUtils.applyBasePathCompletionToMediaItemId(basePathCompletion, SharePointParseUtils.calculateMediaItemId(subSite, mediaItemId));

        String expectedItemId = mediaItemId.toUpperCase() + SharePointParseUtils.SITE_DELIMITER + subSite.toUpperCase() + SharePointParseUtils.BASE_PATH_COMPLETION_DELIMITER + basePathCompletion.toLowerCase();
        assertEquals(expectedItemId, newMediaItemId);

        basePathCompletion = "/" + basePathCompletion;
        newMediaItemId = SharePointParseUtils.applyBasePathCompletionToMediaItemId(basePathCompletion, SharePointParseUtils.calculateMediaItemId(subSite, mediaItemId));
        assertEquals(expectedItemId, newMediaItemId);
    }
}
