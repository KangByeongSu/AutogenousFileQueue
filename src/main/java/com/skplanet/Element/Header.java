package com.skplanet.element;

/**
 * Created by byeongsukang on 2016. 6. 11..
 */
public class Header {
    private int head;
    private int elementCount;
    private int startOffset;
    private int endOffset;

    public Header(int head, int elementCount, int startOffset, int endOffset) {
        this.head = head;
        this.elementCount = elementCount;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public int getHead() {
        return head;
    }

    public void setHead(int head) {
        this.head = head;
    }

    public int getElementCount() {
        return elementCount;
    }

    public void setElementCount(int elementCount) {
        this.elementCount = elementCount;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(int startOffset) {
        this.startOffset = startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(int endOffset) {
        this.endOffset = endOffset;
    }
}
