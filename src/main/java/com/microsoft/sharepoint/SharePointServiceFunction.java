package com.microsoft.sharepoint;

import com.middleware.share.ServiceException;

/**
 * Functional interface that throws ServiceException.
 * @param <T>
 * @param <R>
 */
@FunctionalInterface
public interface SharePointServiceFunction<T, R> {
    R apply (T t) throws ServiceException;
}
