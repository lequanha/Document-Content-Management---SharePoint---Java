package com.microsoft.sharepoint;

import com.middleware.share.Util;
import com.middleware.share.queryoptions.IQueryOption;

public class SharepointPagingToken implements IQueryOption {

    private String lastIdToSkip;
    private int top;

    public SharepointPagingToken(String lastIdToSkip, int top) {
        this.lastIdToSkip = lastIdToSkip;
        this.top = top;
    }

    @Override
    public String toString() {
        if (lastIdToSkip == null) {
           return "$skiptoken=" + Util.encodeUrlInputStream("Paged=TRUE") + "&$top=" + top;
        }
        else {
            return "$skiptoken=" + Util.encodeUrlInputStream("Paged=TRUE&p_ID="+lastIdToSkip) + "&$top=" + top;
        }
    }
}
