package com.microsoft.sharepoint;

import java.util.Objects;

/**
 * Created by uri on 10/11/2016.
 */
public class SharePointExtendedFolder {

    private String listId;

    private String listTitle;

    private Integer itemCount;

    private Integer folderItemCount;

    private String name;
    private String serverRelativeUrl;

    private long creationTime;
    private long lastModifiedTime;

    public String getListId() {
        return listId;
    }

    public void setListId(String listId) {
        this.listId = listId;
    }

    public String getListTitle() {
        return listTitle;
    }

    public void setListTitle(String listTitle) {
        this.listTitle = listTitle;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setServerRelativeUrl(String serverRelativeUrl) {
        this.serverRelativeUrl = serverRelativeUrl;
    }

    public String getServerRelativeUrl() {
        return serverRelativeUrl;
    }

    public Integer getItemCount() {
        return itemCount;
    }

    public void setItemCount(Integer itemCount) {
        this.itemCount = itemCount;
    }

    public Integer getFolderItemCount() {
        return folderItemCount;
    }

    public void setFolderItemCount(Integer folderItemCount) {
        this.folderItemCount = folderItemCount;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SharePointExtendedFolder that = (SharePointExtendedFolder) o;
        return Objects.equals(listId, that.listId) &&
                Objects.equals(listTitle, that.listTitle) &&
                Objects.equals(itemCount, that.itemCount) &&
                Objects.equals(folderItemCount, that.folderItemCount) &&
                Objects.equals(creationTime, that.creationTime) &&
                Objects.equals(lastModifiedTime, that.lastModifiedTime) &&
                Objects.equals(name, that.name) &&
                Objects.equals(serverRelativeUrl, that.serverRelativeUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(listId, listTitle, itemCount, folderItemCount, name, serverRelativeUrl, creationTime, lastModifiedTime);
    }

    @Override
    public String toString() {
        return "SharePointExtendedFolder{" +
                "listId='" + listId + '\'' +
                ", listTitle='" + listTitle + '\'' +
                ", itemCount=" + itemCount +
                ", folderItemCount=" + folderItemCount +
                ", name='" + name + '\'' +
                ", serverRelativeUrl='" + serverRelativeUrl + '\'' +
                ", creationTime='" + creationTime + '\'' +
                ", lastModifiedTime='" + lastModifiedTime + '\'' +
                '}';
    }
}
