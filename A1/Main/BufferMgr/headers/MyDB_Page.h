#ifndef PAGE_H
#define PAGE_H

#include "MyDB_Table.h"

using namespace std;
class MyDB_Page;

class MyDB_Page {
private:
	char* bufferAddr = nullptr;
	pair<MyDB_TablePtr, long> pageId;
	int slotId = 0;
	bool anonymous;
	bool pinned = false;
	bool dirty = false;
	// int refCount = 0;
	size_t pageSize;
public:
	// Initailize anonymous page
	MyDB_Page(size_t pageSize);

	// Initialize non-anonymous page
	MyDB_Page(MyDB_TablePtr whichTable, long i, size_t pageSize);

	char* getBufferAddr();
	void setBufferAddr(char* bufferAddr);

	pair<MyDB_TablePtr, long> getPageId();

	int getSlotId();
	void setSlotId(int slotId);

	bool isAnonymous();

	bool isPinned();
	void setPinned();
	void undoPinned();

	bool isDirty();
	void setDirty(bool dirty);

	size_t getPageSize() const {return pageSize; }

	// int getRefCount();
	// void decRefCount();
	// void incRefCount();
};

#endif