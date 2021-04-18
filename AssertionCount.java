public class AssertionCount {
    private int count;

    public AssertionCount() {
        this.count = 0;
    }

    public void addAssertion() {
        this.count = this.count + 1;
    }

    public int getCount() {
        return this.count;
    }
}
