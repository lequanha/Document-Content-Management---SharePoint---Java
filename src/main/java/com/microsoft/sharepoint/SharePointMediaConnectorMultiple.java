package com.microsoft.sharepoint;

import com.media.SharePointConnectionParametersDto;
import com.StreamMediaItemsParams;
import com.microsoft.MSAppInfo;
import com.microsoft.MSConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SharePointMediaConnectorMultiple extends SharePointMediaConnector {
    private static final String SCAN_MODE_ANNOUNCEMENT = "***~~~******~~~******~~~*** MULTI-MAP MODE - mapping {} times ***~~~******~~~******~~~***";

    private static final Logger logger = LoggerFactory.getLogger(SharePointMediaConnectorMultiple.class);

    private int timesToRescan;


    SharePointMediaConnectorMultiple(SharePointConnectionParametersDto sharePointConnectionDetailsDto, MSAppInfo msAppInfo,
                                     int maxRetries, int pageSize, long maxSupportFileSize, MSConnectionConfig connectionConfig,
                                     int siteCrawlMaxDepth, String folderToFail, int timesToRescan, List<String> foldersToFilter,
                                     boolean isSpecialCharsSupported,
                                     int maxPathCrawlingDepth,
                                     int maxIdenticalNameInPath,
                                     boolean pathMismatchSkip,
                                     String... charsToFilter) {

        super(sharePointConnectionDetailsDto,
                msAppInfo,
                maxRetries,
                pageSize,
                maxSupportFileSize,
                connectionConfig,
                siteCrawlMaxDepth,
                folderToFail,
                foldersToFilter,
                isSpecialCharsSupported,
                maxPathCrawlingDepth,
                maxIdenticalNameInPath,
                pathMismatchSkip,
                charsToFilter);
        this.timesToRescan = timesToRescan;
    }

    @Override
    public void concurrentStreamMediaItems(StreamMediaItemsParams params) {

        logger.error(SCAN_MODE_ANNOUNCEMENT, timesToRescan);

        for (int iter = 0; iter < timesToRescan; iter++) {
            logger.info("Mapping files iteration {} out of {}", iter+1, timesToRescan);
            super.concurrentStreamMediaItems(params);
        }

    }

    @Override
    public void streamMediaItems(StreamMediaItemsParams params) {

        logger.error(SCAN_MODE_ANNOUNCEMENT, timesToRescan);

        for (int iter = 0; iter < timesToRescan; iter++) {
            logger.info("Mapping files iteration {} out of {}", iter+1, timesToRescan);
            super.streamMediaItems(params);
        }
    }
}
