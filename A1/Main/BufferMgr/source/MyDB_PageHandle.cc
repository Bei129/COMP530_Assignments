
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include <iostream>
#include "MyDB_PageHandle.h"

void* MyDB_PageHandleBase::getBytes() {
    if (page->getBufferAddr()==nullptr) {
        if (buffer->bufferSpace.empty()) {
            //cout << "buffer->bufferSpace.empty()" << endl;
            char* newAddr = buffer->evictPage();
            page->setBufferAddr(newAddr);
        }
        else {
            //cout << "else" << endl;
            char* newAddr = buffer->bufferSpace.back();
            buffer->bufferSpace.pop_back();
            page->setBufferAddr(newAddr);
        }
    }
    else {
        //cout << "exists bufferAddr" << endl;
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
}

#endif

