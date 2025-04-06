package org.red5.server.stream;

import java.util.concurrent.CopyOnWriteArrayList;

import org.red5.server.api.stream.IPlayItem;
import org.red5.server.api.stream.support.SimplePlayItem;

public class Item {

    /**
     * List of items in this playlist
     */
    protected CopyOnWriteArrayList<IPlayItem> items;

    /**
     * Current item index
     */
    private int currentItemIndex;

    /**
     * Current item
     */
    protected IPlayItem currentItem;

    /** {@inheritDoc} */
    public void addItem(IPlayItem item) {
        items.add(item);
    }

    /** {@inheritDoc} */
    public void addItem(IPlayItem item, int index) {
        IPlayItem prev = items.get(index);
        if (prev != null && prev instanceof SimplePlayItem) {
            // since it replaces the item in the current spot, reset the items time so the sort will work
            ((SimplePlayItem) item).setCreated(((SimplePlayItem) prev).getCreated() - 1);
        }
        items.add(index, item);
        if (index <= currentItemIndex) {
            // item was added before the currently playing
            currentItemIndex++;
        }
    }

    /** {@inheritDoc} */
    public void removeItem(int index) {
        if (index < 0 || index >= items.size()) {
            return;
        }
        items.remove(index);
        if (index < currentItemIndex) {
            // item was removed before the currently playing
            currentItemIndex--;
        } else if (index == currentItemIndex) {
            // TODO: the currently playing item is removed - this should be handled differently
            currentItemIndex--;
        }
    }

    /** {@inheritDoc} */
    public void removeAllItems() {
        currentItemIndex = 0;
        items.clear();
    }

    /** {@inheritDoc} */
    public int getItemSize() {
        return items.size();
    }

    public CopyOnWriteArrayList<IPlayItem> getItems() {
        return items;
    }

    /** {@inheritDoc} */
    public int getCurrentItemIndex() {
        return currentItemIndex;
    }

    public void setCurrentItemIndex(int index) {
        this.currentItemIndex = index;
    }

    /** {@inheritDoc} */
    public IPlayItem getCurrentItem() {
        return currentItem;
    }

    public void setCurrentItem(IPlayItem item) {
        this.currentItem = item;
    }

    /** {@inheritDoc} */
    public IPlayItem getItem(int index) {
        try {
            return items.get(index);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    /** {@inheritDoc} */
    public boolean hasMoreItems(boolean isRepeat) {
        int nextItem = currentItemIndex + 1;
        if (nextItem >= items.size() && !isRepeat) {
            return false;
        } else {
            return true;
        }
    }
}
