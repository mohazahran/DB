/* File DB.java */

package bufmgr;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import chainexception.ChainException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;

public class BufMgr implements GlobalConst {
	
	private byte[] _bufPool;
	private Descriptor[] _bufDescr;
	String _replacementPolicy;
	AMHash pFHash;
	int _numOfFrames; // total number of frames in buffer pool
	int _numOfUnpinned;// count of used frames with piCount zero
	int _numOfFreeFrames; // count of unused frames
	/**
	 * Create the BufMgr object.
	 * Allocate pages (frames) for the buffer pool in main memory and
	 * make the buffer manage aware that the replacement policy is
	 * specified by replacerArg (e.g., LH, Clock, LRU, MRU, LRFU, etc.).
	 *
	 * @param numbufs number of buffers in the buffer pool
	 * @param lookAheadSize number of pages to be looked ahead, you can ignore that parameter
	 * @param replacementPolicy Name of the replacement policy, that parameter will be set to "LRFU"
	 */
	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {
		_replacementPolicy = replacementPolicy;
		_bufPool = new byte[numbufs*PAGE_SIZE];
		_bufDescr = new Descriptor[numbufs];
		for(int i=0; i<numbufs; i++) {
			_bufDescr[i] = new Descriptor();
		}
		_numOfFrames = numbufs;
		_numOfFreeFrames = numbufs;
		_numOfUnpinned = 0;
		
		pFHash = new AMHash();
	};

	/**
	 * Pin a page.
	 * First check if this page is already in the buffer pool.
	 * If it is, increment the pin_count and return a pointer to this
	 * page.
	 * If the pin_count was 0 before the call, the page was a
	 * replacement candidate, but is no longer a candidate.
	 * If the page is not in the pool, choose a frame (from the
	 * set of replacement candidates) to hold this page, read the
	 * page (using the appropriate method from {\em diskmgr} package) and pin it.
	 * Also, must write out the old page in chosen frame if it is dirty
	 * before reading new page.__ (You can assume that emptyPage==false for
	 * this assignment.)
	 *
	 * @param pageno page number in the Minibase.
	 * @param page the pointer point to the page.
	 * @param emptyPage true (empty page); false (non-empty page)
	 */
	public void pinPage(PageId pageno, Page page, boolean emptyPage) throws BufferPoolExceededException {
		
		int fNumber = pFHash.getEntry(pageno);
		if(fNumber >= 0) {
			_bufDescr[fNumber]._pinCount++;
			if(_bufDescr[fNumber]._pinCount == 1) {
				_numOfUnpinned--;
			}
		}
		else {
			if(_numOfUnpinned + _numOfFreeFrames == 0) {
				throw new BufferPoolExceededException(null, "Buffer Pool Exceeded");
			}
			else if(_numOfFreeFrames > 0) {
				_numOfFreeFrames--;
				for(int i=0; i<_numOfFrames; i++) {
					if(_bufDescr[i]._pId == null) {
						// replace this page
						_bufDescr[i]._pId = pageno;
						_bufDescr[i]._pinCount = 1;
						System.arraycopy(page.getData(), 0, _bufPool, i*PAGE_SIZE, PAGE_SIZE);
						pFHash.insertEntry(pageno,i);
						return;
					}
				}
			}
			else {
				_numOfUnpinned--;
				for(int i=0; i<_numOfFrames; i++) {
					if(_bufDescr[i]._pinCount == 0) {
						// replace this page
						if(_bufDescr[i]._dirtyBit == false) {
							_bufDescr[i]._pId = pageno;
							_bufDescr[i]._pinCount = 1;
							System.arraycopy(page.getData(), 0, _bufPool, i*PAGE_SIZE, PAGE_SIZE);
							pFHash.insertEntry(pageno,i);
							return;
						}
						else {
							try {
								Minibase.DiskManager.write_page(_bufDescr[i]._pId, 
										new Page(Arrays.copyOfRange(_bufPool, i*PAGE_SIZE, (i+1)*PAGE_SIZE)));
							} catch (InvalidPageNumberException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (FileIOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
							_bufDescr[i]._pId = pageno;
							_bufDescr[i]._pinCount = 1;
							_bufDescr[i]._dirtyBit = false;
							System.arraycopy(page.getData(), 0, _bufPool, i*PAGE_SIZE, PAGE_SIZE);
							pFHash.insertEntry(pageno,i);
							return;
						}
					}
				}
			}
		}
	};

	/**
	 * Unpin a page specified by a pageId.
	 * This method should be called with dirty==true if the client has
	 * modified the page.
	 * If so, this call should set the dirty bit
	 * for this frame.
	 * Further, if pin_count>0, this method should
	 * decrement it.
	 *If pin_count=0 before this call, throw an exception
	 * to report error.
	 *(For testing purposes, we ask you to throw
	 * an exception named PageUnpinnedException in case of error.)
	 *
	 * @param pageno page number in the Minibase.
	 * @param dirty the dirty bit of the frame
	 */
	public void unpinPage(PageId pageno, boolean dirty) throws PageUnPinnedException {
		
	};

	/**
	 * Allocate new pages.
	 * Call DB object to allocate a run of new pages and
	 * find a frame in the buffer pool for the first page
	 * and pin it. (This call allows a client of the Buffer Manager
	 * to allocate pages on disk.) If buffer is full, i.e., you
	 * can't find a frame for the first page, ask DB to deallocate
	 * all these pages, and return null.
	 *
	 * @param firstpage the address of the first page.
	 * @param howmany total number of allocated new pages.
	 *
	 * @return the first page id of the new pages.__ null, if error.
	 */
	public PageId newPage(Page firstpage, int howmany) {
		return null;
	};

	/**
	 * This method should be called to delete a page that is on disk.
	 * This routine must call the method in diskmgr package to
	 * deallocate the page.
	 *
	 * @param globalPageId the page number in the data base.
	 */
	public void freePage(PageId globalPageId) throws PagePinnedException 
        {
            
        };

	/**
	 * Used to flush a particular page of the buffer pool to disk.
	 * This method calls the write_page method of the diskmgr package.
	 *
	 * @param pageid the page number in the database.
	 */
	public void flushPage(PageId pageid) {};

	/**
	 * Used to flush all dirty pages in the buffer pool to disk
	 *
	 */
	public void flushAllPages() {};

	/**
	 * Returns the total number of buffer frames.
	 */
	public int getNumBuffers() {
		return _numOfFrames;
	}

	/**
	 * Returns the total number of unpinned buffer frames.
	 */
	public int getNumUnpinned() {
		return _numOfUnpinned;
	}

};
