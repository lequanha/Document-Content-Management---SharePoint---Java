package com.microsoft.sharepoint;

import com.microsoft.services.orc.core.BaseOrcContainer;
import com.microsoft.services.orc.core.DependencyResolver;

/**
 * Created by uri on 10/11/2016.
 */
public class SharePointContainerClient extends BaseOrcContainer {

    public SharePointContainerClient(String url, DependencyResolver resolver) {
        super(url, resolver);
    }


}
