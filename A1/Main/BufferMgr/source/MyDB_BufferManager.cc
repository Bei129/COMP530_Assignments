
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
    newPageObj->incRefCount();
    MyDB_PageHandle newPage = make_shared<MyDB_PageHandleBase>(newPageObj, this);
    pageMap[pageId] = newPage;
    lruList.push_front(newPage);
    if (lruList.size() > numPages) {
        evictPage();
    }
    return newPage;
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	MyDB_Page* tempPageObj = new MyDB_Page(pageSize);
    if (anonymousSpace.empty()) {
        tempPageObj->setSlotId(slotId++);
    }
    else {
        tempPageObj->setSlotId(anonymousSpace.back());
        anonymousSpace.pop_back();
    }
    tempPageObj->incRefCount();
    MyDB_PageHandle tempPage = make_shared<MyDB_PageHandleBase>(tempPageObj, this);
    lruList.push_front(tempPage);
    if (lruList.size() > numPages) {
        evictPage();
    }
    return tempPage;
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long i) {
	MyDB_PageHandle ph = getPage(whichTable, i);
    ph->getPage()->setPinned();
    return ph;
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	MyDB_PageHandle ph = getPage();
    ph->getPage()->setPinned(); 
    return ph;	
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
    unpinMe->getPage()->undoPinned();
}

MyDB_BufferManager::MyDB_BufferManager(size_t pageSize, size_t numPages, string tempFile)
    : pageSize(pageSize), numPages(numPages), tempFile(tempFile) {
    pageMap = unordered_map<pair<MyDB_TablePtr, long>, MyDB_PageHandle, pair_hash>();
    buffer = (char*)malloc(pageSize * numPages);
    char* addr = buffer;
    for (int i = 0; i < numPages; i++) {
        bufferSpace.push_back(addr);
        addr += pageSize;
    }
}

MyDB_BufferManager::~MyDB_BufferManager() {
    for (auto &page : lruList) {
        writeToDisk(page->getPage());
    }
    free(buffer);
}

char* MyDB_BufferManager::evictPage() {
    MyDB_PageHandle lruPage = lruList.back();
    char* evictAddr = lruPage->getPage()->getBufferAddr();
    auto rit = lruList.rbegin();
    while(rit != lruList.rend()) {
        lruPage = *rit;

        if (!lruPage->isPinned()) {
            auto it = pageMap.find(lruPage->getPageId());
            if (it != pageMap.end()) {
                pageMap.erase(it);
            }

            writeToDisk(lruPage->getPage());

            evictAddr = lruPage->getPage()->getBufferAddr();

            lruPage->getPage()->setBufferAddr(nullptr);

            lruList.erase(std::next(rit).base());

            break;
        }
        ++rit;
    }

    return evictAddr;
}

void MyDB_BufferManager::readFromDisk(MyDB_Page* page) {
    if (page->isAnonymous()) {
        int fd = open(tempFile.c_str(), O_RDONLY | O_CREAT | O_FSYNC, 0666);
        if (fd != -1) {
            lseek(fd, page->getSlotId() * pageSize, SEEK_SET);
            read(fd, page->getBufferAddr(), pageSize);
            close(fd);
            page->setDirty(false);
        }
    }
    else {
        int fd = open(page->getPageId().first->getStorageLoc().c_str(), O_RDONLY | O_CREAT | O_FSYNC, 0666);
        if (fd != -1) {
            lseek(fd, page->getPageId().second * pageSize, SEEK_SET);
            read(fd, page->getBufferAddr(), pageSize);
            close(fd);
            page->setDirty(false);
        }
    }
}


void MyDB_BufferManager::writeToDisk(MyDB_Page* page) {
    if (page->isAnonymous()) {
        if (page->isDirty()) {
            int fd = open(tempFile.c_str(), O_WRONLY | O_CREAT | O_FSYNC, 0666);
            if (fd != -1) {
                lseek(fd, page->getSlotId() * pageSize, SEEK_SET);
                write(fd, page->getBufferAddr(), pageSize);
                close(fd);
                page->setDirty(false);
            }
        }
    }
    else {
        if (page->isDirty()) {
            int fd = open(page->getPageId().first->getStorageLoc().c_str(), O_WRONLY | O_CREAT | O_FSYNC, 0666);
            if (fd != -1) {
                lseek(fd, page->getPageId().second * pageSize, SEEK_SET);
                write(fd, page->getBufferAddr(), pageSize);
                close(fd);
                page->setDirty(false);
            }
        }
    }
}

#endif
