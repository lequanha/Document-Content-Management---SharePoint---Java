package com.microsoft.sharepoint;

import com.StreamMediaItemsParams;

import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public class StreamListData {
    private String path;
    private final StreamMediaItemsParams params;
    private final String listId;
    private String libName;
    private final String subSite;

    public StreamListData(String path, String listId, String libName, String subSite,
                          StreamMediaItemsParams params) {
        this.path = path;
        this.params = params;
        this.listId = listId;
        this.subSite = subSite;
        this.libName = libName;
    }

    public String getPath() {
        return path;
    }

    public StreamMediaItemsParams getParams() {
        return params;
    }

    public String getListId() {
        return listId;
    }

    public String getLibName() {
        return libName;
    }

    public String getSubSite() {
        return subSite;
    }

    public void appendToPath(String addendum) {
        this.path += addendum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamListData that = (StreamListData) o;
        return Objects.equals(path, that.path) &&
                Objects.equals(params, that.params) &&
                Objects.equals(listId, that.listId) &&
                Objects.equals(libName, that.libName) &&
                Objects.equals(subSite, that.subSite);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path,params, listId, libName, subSite);
    }
}
