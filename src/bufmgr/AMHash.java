package bufmgr;

import global.PageId;

import java.util.ArrayList;


public class AMHash {
	
	private ArrayList<BucketEntry>[] _buckets;
	private int a = 997; // prime
	private int b = 463; // prime
	private int HTSIZE = 839; // prime	
	
	@SuppressWarnings("unchecked")
	public AMHash() {
		_buckets = (ArrayList<BucketEntry>[])new ArrayList[HTSIZE];
		for(int i=0; i< HTSIZE; i++) {
			_buckets[i] = new ArrayList<BucketEntry>();
		}
	}
	
	public boolean insertEntry(PageId pageNumber, int frameNumber) {
		int hValue = a*pageNumber.pid + b;
		hValue = hValue % HTSIZE;
		
		if(getEntry(pageNumber) >= 0) {
			return false;
		}
		
		_buckets[hValue].add(new BucketEntry(pageNumber,frameNumber));
		return true;
	}
	
	public boolean removeEntry(PageId pageNumber) {
		int hValue = a*pageNumber.pid + b;
		hValue = hValue % HTSIZE;
		
		ArrayList<BucketEntry> temp = _buckets[hValue];
		for(int i=0; i< temp.size(); i++) {
			BucketEntry be = (BucketEntry)temp.get(i);
			if(be._pageNumber.pid == pageNumber.pid) {
				_buckets[hValue].remove(i);
				return true;
			}
		}
		
		return false;
	}
	
	public int getEntry(PageId pageNumber) {
		int hValue = a*pageNumber.pid + b;
		hValue = hValue % HTSIZE;
		
		ArrayList<BucketEntry> temp = _buckets[hValue];
		for(int i=0; i< temp.size(); i++) {
			BucketEntry be = (BucketEntry)temp.get(i);
			if(be._pageNumber.pid == pageNumber.pid) {
				
				return be._frameNumber;				
			}
		}
		return -1;
	}
	
	public static void main (String argv[]) {

		AMHash testH = new AMHash();
		testH.insertEntry(new PageId(1), 3);
		testH.removeEntry(new PageId(1));
		testH.insertEntry(new PageId(1), 0);
		int f = testH.getEntry(new PageId(1));
		int ff = f + 2;
		
	}
}
