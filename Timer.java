
public class Timer {
	private boolean mExpired;
	public Timer() {
		mExpired = false;
	}
	
	public void resetTimer() {
		mExpired = false;
	}
	
	public void expireTimer() {
		mExpired = true;
	}
	
	public boolean getTimerValue() {
		return mExpired;
	}

}
