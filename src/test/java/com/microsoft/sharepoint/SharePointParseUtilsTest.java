package com.microsoft.sharepoint;

import junit.framework.TestCase;
import org.junit.Test;

public class SharePointParseUtilsTest extends TestCase {

    @Test
    public void testEncodeSubSiteNameIfNeeded() {
        assertEquals("CDO", SharePointParseUtils.encodeSubSiteNameIfNeeded("CDO"));
        assertEquals("CDO/abc", SharePointParseUtils.encodeSubSiteNameIfNeeded("CDO/abc"));
        assertEquals("/CDO/abc/", SharePointParseUtils.encodeSubSiteNameIfNeeded("/CDO/abc/"));
        assertEquals("CDO/g%20h", SharePointParseUtils.encodeSubSiteNameIfNeeded("CDO/g h"));
        assertEquals("CD%20O/gh", SharePointParseUtils.encodeSubSiteNameIfNeeded("CD O/gh"));
        assertEquals("CD%20O/g%20h/hh/", SharePointParseUtils.encodeSubSiteNameIfNeeded("CD O/g h/hh/"));
        assertEquals("CD%20O/g%20h/%5bhh%5d/", SharePointParseUtils.encodeSubSiteNameIfNeeded("CD O/g h/[hh]/"));
    }
}