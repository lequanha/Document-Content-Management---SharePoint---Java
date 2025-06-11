package com.microsoft.sharepoint;

import com.middleware.share.Util;
import com.middleware.share.queryoptions.IFilterRestriction;

public class SharePointStartsWithRestriction implements IFilterRestriction {

    private String propertyName;
    private String value;

    public SharePointStartsWithRestriction(String propertyName, String value) {
        if(propertyName == null) {
            throw new IllegalArgumentException("propertyName");
        } else if(value == null) {
            throw new IllegalArgumentException("value");
        } else {
            this.propertyName = propertyName;
            this.value = value;
        }
    }

    public String toString() {
        return "startswith(" + Util.encodeUrlInputStream(this.propertyName) + ", '" + this.value + "')";
    }

}
