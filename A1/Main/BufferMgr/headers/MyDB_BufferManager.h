
#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include "MyDB_Table.h"
#include "CheckLRU.h"
#include "MyDB_Page.h"
#include <unordered_map>
#include <vector>
#include <list>
#include <string>

using namespace std;

class MyDB_PageHandleBase;
typedef shared_ptr<MyDB_PageHandleBase> MyDB_PageHandle;

struct pair_hash {
    template <class T1, class T2>
    std::size_t operator() (const std::pair<T1, T2>& pair) const {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};

class MyDB_BufferManager {

public:

	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle 
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular 
	// table
	MyDB_PageHandle getPage ();

	// gets the i^th page in the table whichTable... the only difference 
	// between this method and getPage (whicTable, i) is that the page will be 
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage ();

	// un-pins the specified page
	void unpin (MyDB_PageHandle unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize 
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
	
	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager ();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS 
	char* evictPage();
    void writeToDisk(MyDB_Page* page);
    void readFromDisk(MyDB_Page* page);

	vector<char*> bufferSpace;
	vector<int> anonymousSpace;

	// LRU
	void update(shared_ptr<MyDB_Page> page);
    void insert(shared_ptr<MyDB_Page> page);

	bool isValidBufferAddr(char* addr) const { 
		return addr >= buffer && addr < (buffer + pageSize * numPages);
	}


private:

	// YOUR STUFF HERE
	size_t pageSize;  
	size_t numPages;
	string tempFile; 
	char* buffer;

	// index temp file
	int slotId = 0;

	// unordered_map<pair<MyDB_TablePtr, long>, MyDB_Page*, pair_hash> pageMap;
	// list<MyDB_Page*> lruList;
	unordered_map<pair<MyDB_TablePtr, long>, std::shared_ptr<MyDB_Page>, pair_hash> pageMap;
    list<std::shared_ptr<MyDB_Page>> lruList;
	//LRU* lru;


};

#endif


