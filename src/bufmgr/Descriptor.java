package bufmgr;

import java.util.ArrayList;

import global.PageId;

public class Descriptor {
	
	public PageId _pId;
	public int _pinCount;
	public boolean _dirtyBit;
	public ArrayList<Integer> _times;
	
	public Descriptor() {
		_pId = null;
		_pinCount = -1;
		_dirtyBit = false;
		_times = new ArrayList<>();
	}
	
	public Descriptor (PageId pId, int pinCount, boolean dirtyBit) {
		_pId = pId;
		_pinCount = pinCount;
		_dirtyBit = dirtyBit;
		_times = new ArrayList<>();
	}
	
	public void clear() {
		_pId = null;
		_pinCount = -1;
		_dirtyBit = false;
		_times = new ArrayList<>();
	}
};