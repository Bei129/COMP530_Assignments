
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include <iostream>
#include "MyDB_PageHandle.h"

void* MyDB_PageHandleBase::getBytes() {
    if (page->getBufferAddr() == nullptr) {
        if (buffer->bufferSpace.empty()) {
            char* newAddr = buffer->evictPage();
            if (!buffer->isValidBufferAddr(newAddr)) {
                cerr << "Error: Invalid buffer address assigned." << endl;
                exit(1);
            }
            page->setBufferAddr(newAddr);
            buffer->insert(page);
        }
        else {
            char* newAddr = buffer->bufferSpace.back();
            buffer->bufferSpace.pop_back();
            if (!buffer->isValidBufferAddr(newAddr)) {
                cerr << "Error: Invalid buffer address popped from bufferSpace." << endl;
                exit(1);
            }
            page->setBufferAddr(newAddr);
            buffer->insert(page);
        }
    }
    else {
        buffer->update(page);
    }
    return page->getBufferAddr();
}

void MyDB_PageHandleBase::wroteBytes() {
    page->setDirty(true);
}

MyDB_PageHandleBase::MyDB_PageHandleBase(std::shared_ptr<MyDB_Page> page, MyDB_BufferManager* buffer) 
    : page(page), buffer(buffer) {
    if (!page) {
        cerr << "Error: Null page pointer passed to MyDB_PageHandleBase!" << endl;
        exit(1); 
    }
}

MyDB_PageHandleBase::~MyDB_PageHandleBase() {
    if (page->isAnonymous() && page->getBufferAddr() == nullptr) {
        buffer->anonymousSpace.push_back(page->getSlotId());
        // cout << "Recycled slotId " << page->getSlotId() << " back to anonymousSpace." << endl;
    }
}

#endif

