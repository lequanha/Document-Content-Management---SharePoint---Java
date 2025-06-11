package com.microsoft.sharepoint;

import com.microsoft.services.orc.core.BaseOrcContainer;
import com.microsoft.services.orc.core.DependencyResolver;

public class SharePointContainerClient extends BaseOrcContainer {

    public SharePointContainerClient(String url, DependencyResolver resolver) {
        super(url, resolver);
    }


}
