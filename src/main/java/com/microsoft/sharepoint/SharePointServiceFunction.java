package com.microsoft.sharepoint;

import com.middleware.share.ServiceException;

/**
 * Created by oren on 12/3/2017.
 */

/**
 * Functional interface that throws ServiceException.
 * @param <T>
 * @param <R>
 */
@FunctionalInterface
public interface SharePointServiceFunction<T, R> {
    R apply (T t) throws ServiceException;
}
