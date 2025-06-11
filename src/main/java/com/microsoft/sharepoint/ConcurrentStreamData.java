package com.microsoft.sharepoint;

import com.messages.DirListingPayload;

import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

public class ConcurrentStreamData<T> {

    private ForkJoinPool forkJoinPool;
    private T streamSiteData;
    private Consumer<DirListingPayload> directoryListingConsumer;

    public ConcurrentStreamData(ConcurrentStreamData source, T streamSiteData) {
        this(source.forkJoinPool, streamSiteData, source.directoryListingConsumer);
    }

    public ConcurrentStreamData(ForkJoinPool forkJoinPool, T streamSiteData, Consumer<DirListingPayload> directoryListingConsumer) {
        this.forkJoinPool = forkJoinPool;
        this.streamSiteData = streamSiteData;
        this.directoryListingConsumer = directoryListingConsumer;
    }

    public ForkJoinPool getForkJoinPool() {
        return forkJoinPool;
    }

    public T getStreamData() {
        return streamSiteData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConcurrentStreamData<?> that = (ConcurrentStreamData<?>) o;
        return Objects.equals(forkJoinPool, that.forkJoinPool) &&
                Objects.equals(streamSiteData, that.streamSiteData) &&
                Objects.equals(directoryListingConsumer, that.directoryListingConsumer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(forkJoinPool, streamSiteData, directoryListingConsumer);
    }

    public Consumer<DirListingPayload> getDirectoryListingConsumer() {
        return directoryListingConsumer;
    }

    @Override
    public String toString() {
        return "ConcurrentStreamData{" +
                "forkJoinPool=" + forkJoinPool +
                ", streamSiteData=" + streamSiteData +
                ", directoryListingConsumer=" + directoryListingConsumer +
                '}';
    }
}
