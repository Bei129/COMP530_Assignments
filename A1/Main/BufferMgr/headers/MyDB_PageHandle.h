
#ifndef PAGE_HANDLE_H
#define PAGE_HANDLE_H

#include <memory>
#include "MyDB_BufferManager.h"
#include "MyDB_Page.h"

// page handles are basically smart pointers
using namespace std;
class MyDB_PageHandleBase;
typedef shared_ptr <MyDB_PageHandleBase> MyDB_PageHandle;

class MyDB_PageHandleBase {

public:

	// THESE METHODS MUST BE IMPLEMENTED WITHOUT CHANGING THE DEFINITION

	// access the raw bytes in this page... if the page is not currently
	// in the buffer, then the contents of the page are loaded from 
	// secondary storage. 
	void *getBytes ();

	// let the page know that we have written to the bytes.  Must always
	// be called once the page's bytes have been written.  If this is not
	// called, then the page will never be marked as dirty, and the page
	// will never be written to disk. 
	void wroteBytes ();

	// There are no more references to the handle when this is called...
	// this should decrmeent a reference count to the number of handles
	// to the particular page that it references.  If the number of 
	// references to a pinned page goes down to zero, then the page should
	// become unpinned.  
	~MyDB_PageHandleBase ();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS
	MyDB_PageHandleBase(std::shared_ptr<MyDB_Page> page, MyDB_BufferManager* buffer);
	bool isDirty() const { return page->isDirty(); }
    bool isPinned() const { return page->isPinned(); } 
    int getSlotId() const { return page->getSlotId(); }
    std::pair<MyDB_TablePtr, long> getPageId() const { return page->getPageId(); }
	std::shared_ptr<MyDB_Page> getPage() const { return page; }
private:

	// YOUR CODE HERE
	std::shared_ptr<MyDB_Page> page; 
    MyDB_BufferManager* buffer;
	
};

#endif

