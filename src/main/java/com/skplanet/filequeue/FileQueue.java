package com.skplanet.filequeue;

/**
 * Created by byeongsukang on 2016. 6. 6..
 */
public interface FileQueue {

    /**
     * Inserts the specified element into this queue if it is possible to do so immediately without violating capacity restrictions,
     * returning true upon success and throwing an IllegalStateException if no space is currently available.
     */
    void add();

    /**
     * Inserts the specified element into this queue if it is possible to do so immediately without violating capacity restrictions.
     * When using a capacity-restricted queue, this method is generally preferable to add(E), which can fail to insert an element only by throwing an exception.
     */
    void offer();

    /**
     * Retrieves and removes the head of this queue. This method differs from poll only in that it throws an exception if this queue is empty.
     */
    void remove();

    /**
     * Retrieves and removes the head of this queue, or returns null if this queue is empty.
     */
    void poll();

    /**
     * Retrieves, but does not remove, the head of this queue. This method differs from peek only in that it throws an exception if this queue is empty.
     */
    void element();

    /**
     * Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
     */
    void peek();


    /**
     * Close
     */
    void close();
}
