package com.microsoft.sharepoint;

import com.acl.AclType;

public class SharePointRoleAssignment {

    private String loginName;

    private AclType aclType;

    public SharePointRoleAssignment() {

    }

    public String getLoginName() {
        return loginName;
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    public AclType getAclType() {
        return aclType;
    }

    public void setAclType(AclType aclType) {
        this.aclType = aclType;
    }

    @Override
    public String toString() {
        return "SharePointRoleAssignment{" +
                "loginName='" + loginName + '\'' +
                ", aclType=" + aclType +
                '}';
    }
}
