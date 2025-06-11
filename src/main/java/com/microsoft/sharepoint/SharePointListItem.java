package com.microsoft.sharepoint;

import com.middleware.share.FileSystemObjectType;

/**
 * Created by uri on 06/10/2016.
 */
public class SharePointListItem {

    String fileRef;

    String id;

    boolean listItemHavingUniqueAcls;

    FileSystemObjectType fileSystemObjectType;
    private String authorId;
    private String modified;
    private String created;
    private String loginName;
    private Long size;

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SharePointListItem{");
        sb.append("fileRef='").append(fileRef).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", listItemHavingUniqueAcls=").append(listItemHavingUniqueAcls);
        sb.append(", fileSystemObjectType=").append(fileSystemObjectType);
        sb.append(", authorId='").append(authorId).append('\'');
        sb.append(", modified='").append(modified).append('\'');
        sb.append(", created='").append(created).append('\'');
        sb.append(", loginName='").append(loginName).append('\'');
        sb.append(", size=").append(size);
        sb.append('}');
        return sb.toString();
    }
    // Todo: add support for access time

    public String getFileRef() {
        return fileRef;
    }

    public void setFileRef(String fileRef) {
        this.fileRef = fileRef;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isListItemHavingUniqueAcls() {
        return listItemHavingUniqueAcls;
    }

    public void setListItemHavingUniqueAcls(boolean listItemHavingUniqueAcls) {
        this.listItemHavingUniqueAcls = listItemHavingUniqueAcls;
    }

    public FileSystemObjectType getFileSystemObjectType() {
        return fileSystemObjectType;
    }

    public void setFileSystemObjectType(FileSystemObjectType fileSystemObjectType) {
        this.fileSystemObjectType = fileSystemObjectType;
    }

    public void setAuthorId(String authorId) {
        this.authorId = authorId;
    }

    public String getAuthorId() {
        return authorId;
    }

    public void setModified(String modified) {
        this.modified = modified;
    }

    public String getModified() {
        return modified;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getCreated() {
        return created;
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    public String getLoginName() {
        return loginName;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }
}
