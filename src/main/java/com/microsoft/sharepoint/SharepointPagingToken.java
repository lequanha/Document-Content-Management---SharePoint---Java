package com.microsoft.sharepoint;

import com.middleware.share.Util;
import com.middleware.share.queryoptions.IQueryOption;

/**
 * Created by uri on 23/08/2016.
 */
public class SharepointPagingToken implements IQueryOption {

    private String lastIdToSkip;
    private int top;

//    private int sortBehavior;

    public SharepointPagingToken(String lastIdToSkip, int top) {
        this.lastIdToSkip = lastIdToSkip;
        this.top = top;
//        this.sortBehavior = 1;
    }

    @Override
    public String toString() {
        if (lastIdToSkip == null) {
//            return "$top="+top;
//            return "$skiptoken=" + Util.encodeUrlInputStream("Paged=TRUE&p_SortBehavior="+sortBehavior) + "&$top=" + top;
            return "$skiptoken=" + Util.encodeUrlInputStream("Paged=TRUE") + "&$top=" + top;
        }
        else {
            //$skiptoken=Paged=TRUE&p_ID=5
            //    var endpointUrl = $skiptoken=" + encodeURIComponent('Paged=TRUE&p_SortBehavior=0&p_ID=' + (startItemId-1) + '&$top=' + itemsCount);
//            return "$skiptoken=" + Util.encodeUrlInputStream("Paged=TRUE&p_SortBehavior="+sortBehavior+"&p_ID="+lastIdToSkip) + "&$top=" + top;
            return "$skiptoken=" + Util.encodeUrlInputStream("Paged=TRUE&p_ID="+lastIdToSkip) + "&$top=" + top;
        }
    }
}
