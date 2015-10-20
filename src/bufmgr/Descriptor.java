package bufmgr;

import global.PageId;

public class Descriptor {
	
	public PageId _pId;
	public int _pinCount;
	public boolean _dirtyBit;
	
	public Descriptor() {
		_pId = null;
		_pinCount = -1;
		_dirtyBit = false;
	}
	
	public Descriptor (PageId pId, int pinCount, boolean dirtyBit) {
		_pId = pId;
		_pinCount = pinCount;
		_dirtyBit = dirtyBit;
	}
};