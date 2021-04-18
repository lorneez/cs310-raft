import java.util.ArrayList;

public class RaftLog {

    private ArrayList<Entry> mEntries;

    public RaftLog(Entry[] entries) {
        mEntries = new ArrayList<Entry>();
        for (int i = 0; i < entries.length; i++) {
            mEntries.add(entries[i]);
        }
    }

    public int getLastEntryIndex() {
        return mEntries.size() - 1;
    }

    public int getLastEntryTerm() {
        if (mEntries.size() == 0) {
            return -1;
        }
        return mEntries.get(getLastEntryIndex()).term;
    }

    public Entry getEntry(int index) {
        if (index < 0 || index >= mEntries.size()) {
            return null;
        }
        return mEntries.get(index);
    }

    /**
     * @param entries   to append (in order of 0 to append.length-1). must
     *                  be non-null.
     * @param prevIndex index of log entry before entries to append (-1 if
     *                  inserting at index 0)
     * @return highest index in log after entries have been appended, if
     * the entry at prevIndex is not from prevTerm or if the log does
     * not have an entry at prevIndex, the append request will fail, and
     * the method will return -1.
     */
    public int insert(Entry[] entries, int prevIndex) {
        if (prevIndex < -1 || prevIndex >= mEntries.size()) {
            return -1;
        }
        while (mEntries.size() > prevIndex + 1) {
            mEntries.remove(prevIndex + 1);
        }
        for (int i = 0; i < entries.length; i++) {
            mEntries.add(entries[i]);
        }
        return getLastEntryIndex();
    }

    public void printAllEntries() {
        for (int i = 0; i < mEntries.size(); i++) {
            System.out.println(mEntries.get(i).term + " " + mEntries.get(i).action);
        }
    }
}
