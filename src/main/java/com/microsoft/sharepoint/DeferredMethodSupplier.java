package com.microsoft.sharepoint;

public interface DeferredMethodSupplier<T, E extends Exception> {

    T invoke() throws E;
}