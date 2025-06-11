package com.microsoft.sharepoint;

import com.middleware.share.ChangeToken;
import com.middleware.share.ChangeTokenScope;

import java.io.Serializable;

public class SharePointChangeTokenDto implements Serializable {


    private ChangeTokenScope scope;
    private int version;
    private String scopeId;
    private long date;
    private int changeNumber;

    public SharePointChangeTokenDto() {
    }

    public SharePointChangeTokenDto(ChangeToken token) {
        this.scopeId = token.getScopeId();
        this.date = token.getChangeTime().getTime();
        this.changeNumber = token.getChangeNumber();
        this.version = token.getVersion();
        this.scope = token.getScope();
    }

    public String getScopeId() {
        return scopeId;
    }

    public void setScopeId(String scopeId) {
        this.scopeId = scopeId;
    }

    public ChangeTokenScope getScope() {
        return scope;
    }

    public void setScope(ChangeTokenScope scope) {
        this.scope = scope;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public int getChangeNumber() {
        return changeNumber;
    }

    public void setChangeNumber(int changeNumber) {
        this.changeNumber = changeNumber;
    }
}
