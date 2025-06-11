package com.microsoft.sharepoint;

import com.StreamMediaItemsParams;

import java.util.Objects;

public class StreamSiteData {
    private StreamMediaItemsParams params;
    private String path;
    private String subSite;
    private int currentDepth;

    StreamSiteData(StreamMediaItemsParams params, String path, String subSite, int currentDepth) {
        this.params = params;
        this.path = path;
        this.subSite = subSite;
        this.currentDepth = currentDepth;
    }

    public StreamMediaItemsParams getParams() {
        return params;
    }

    public String getPath() {
        return path;
    }

    public String getSubSite() {
        return subSite;
    }

    public int getCurrentDepth() {
        return currentDepth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamSiteData that = (StreamSiteData) o;
        return currentDepth == that.currentDepth &&
                Objects.equals(params, that.params) &&
                Objects.equals(path, that.path) &&
                Objects.equals(subSite, that.subSite);
    }

    @Override
    public int hashCode() {
        return Objects.hash(params, path, subSite, currentDepth);
    }

    @Override
    public String toString() {
        return "StreamSiteData{" +
                "params=" + params +
                ", path='" + path + '\'' +
                ", subSite='" + subSite + '\'' +
                ", currentDepth=" + currentDepth +
                '}';
    }
}
