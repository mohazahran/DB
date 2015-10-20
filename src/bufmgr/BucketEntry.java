package bufmgr;

import global.PageId;

public class BucketEntry {
	
	public PageId _pageNumber;
	public int _frameNumber;
	
	public BucketEntry() {
		_pageNumber = null;
		_frameNumber = -1;
	}
	
	public BucketEntry(PageId pageNumber, int frameNumber) {
		_pageNumber = pageNumber;
		_frameNumber = frameNumber;
	}
}
