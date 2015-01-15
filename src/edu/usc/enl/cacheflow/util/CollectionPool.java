package edu.usc.enl.cacheflow.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/27/12
 * Time: 10:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class CollectionPool<T extends Collection> {
    private final List<TempCollection<T>> tempCollectionLocks = new LinkedList<>();
    private final T template;

    public CollectionPool(T template) {
        this.template = template;
    }

    public synchronized TempCollection<T> getTempCollection() {
        for (TempCollection<T> tempCollectionLock : tempCollectionLocks) {
            if (tempCollectionLock.tryLock()) {
                return tempCollectionLock;
            }
        }

        final TempCollection<T> output = new TempCollection<T>(((T) Util.getNewCollectionInstance(template)), new ReentrantLock());
        output.tryLock();
        tempCollectionLocks.add(output);
        return output;
    }

    public static class TempCollection<R extends Collection> {

        private final R data;
        private final Lock lock;

        public TempCollection(R data, Lock lock) {
            this.data = data;
            this.lock = lock;
        }

        public void release() {
            data.clear();
            lock.unlock();
        }

        private boolean tryLock() {
            return lock.tryLock();
        }

        public R getData() {
            return data;
        }

        public R cloneData() {
            final R newCollectionInstance = ((R) Util.getNewCollectionInstance(data));
            newCollectionInstance.addAll(data);
            return newCollectionInstance;
        }
    }
}
