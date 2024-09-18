
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include "MyDB_PageHandle.h" 
#include <string>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <algorithm>

using namespace std;

MyDB_PageHandle MyDB_BufferManager::getPage(MyDB_TablePtr whichTable, long i) {
    pair<MyDB_TablePtr, long> pageId = make_pair(whichTable, i);
    auto it = pageMap.find(pageId);
    if (it != pageMap.end()) {
        auto listIt = find(lruList.begin(), lruList.end(), it->second);
        lruList.splice(lruList.begin(), lruList, listIt);
        return it->second;
    }
    MyDB_Page* newPageObj = new MyDB_Page(whichTable, i, pageSize);
    newPageObj->setBufferAddr(new char[pageSize]); 
    MyDB_PageHandle newPage = make_shared<MyDB_PageHandleBase>(newPageObj);
    pageMap[pageId] = newPage;
    lruList.push_front(newPage);
    if (lruList.size() > numPages) {
        evictPage();
    }
    return newPage;
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	MyDB_Page* tempPageObj = new MyDB_Page(pageSize); 
    MyDB_PageHandle tempPage = make_shared<MyDB_PageHandleBase>(tempPageObj);
    lruList.push_front(tempPage);
    if (lruList.size() > numPages) {
        evictPage();
    }
    return tempPage;
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long i) {
	MyDB_PageHandle page = getPage(whichTable, i);
    page->getBytes();
    return page;
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	MyDB_PageHandle page = getPage();
    page->getBytes(); 
    return page;	
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
}

MyDB_BufferManager::MyDB_BufferManager(size_t pageSize, size_t numPages, string tempFile)
    : pageSize(pageSize), numPages(numPages), tempFile(tempFile) {}

MyDB_BufferManager::~MyDB_BufferManager() {
    for (auto &page : lruList) {
        if (page->isDirty()) {
            int fd = open(tempFile.c_str(), O_WRONLY | O_CREAT, 0666);
            if (fd != -1) {
                lseek(fd, page->getSlotId() * pageSize, SEEK_SET);
                write(fd, page->getBytes(), pageSize);
                close(fd);
            }
        }
    }
}

void MyDB_BufferManager::evictPage() {
    MyDB_PageHandle lruPage = lruList.back();
    if (!lruPage->isPinned()) {
        lruList.pop_back();
        auto it = pageMap.find(lruPage->getPageId());
        if (it != pageMap.end()) {
            delete[] static_cast<char*>(lruPage->getBytes());
            pageMap.erase(it);
        }
        if (lruPage->isDirty()) {
            int fd = open(tempFile.c_str(), O_WRONLY | O_CREAT, 0666);
            if (fd != -1) {
                lseek(fd, lruPage->getSlotId() * pageSize, SEEK_SET);
                write(fd, lruPage->getBytes(), pageSize);
                close(fd);
            }
        }
    }
}
	
#endif
