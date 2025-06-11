package com.microsoft.sharepoint;

import java.io.Serializable;
import java.util.List;

public class SharePointListItemPage implements Serializable {

    List<SharePointListItem> items;

    String nextUrl;

    public SharePointListItemPage() {
    }

    public SharePointListItemPage(List<SharePointListItem> result) {
        this.items = result;
    }

    public List<SharePointListItem> getItems() {
        return items;
    }

    public void setItems(List<SharePointListItem> items) {
        this.items = items;
    }

    public String getNextUrl() {
        return nextUrl;
    }

    public void setNextUrl(String nextUrl) {
        this.nextUrl = nextUrl;
    }
}
