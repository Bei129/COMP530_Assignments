
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include <iostream>
#include "MyDB_PageHandle.h"

void* MyDB_PageHandleBase::getBytes() {
    if (page->getBufferAddr()==nullptr) {
        if (buffer->bufferSpace.empty()) {
            char* newAddr = buffer->evictPage();
            page->setBufferAddr(newAddr);
        }
        else {
            char* newAddr = buffer->bufferSpace.back();
            buffer->bufferSpace.pop_back();
            page->setBufferAddr(newAddr);
        }
    }
    return page->getBufferAddr();
}

void MyDB_PageHandleBase::wroteBytes() {
    page->setDirty(true);
}

MyDB_PageHandleBase::MyDB_PageHandleBase(MyDB_Page* page, MyDB_BufferManager* buffer) : 
    page(page), buffer(buffer) {
    if (!page) {
        cerr << "Error: Null page pointer passed to MyDB_PageHandleBase!" << endl;
        exit(1); 
    }
}

MyDB_PageHandleBase :: ~MyDB_PageHandleBase () {
    page->decRefCount();
    if (page->getRefCount() == 0) {
        page->undoPinned();
        if (page->isAnonymous()) {
            this->buffer->anonymousSpace.push_back(page->getSlotId());
        }
    }
}

#endif

