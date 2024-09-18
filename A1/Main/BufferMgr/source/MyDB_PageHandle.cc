
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include <iostream>
#include "MyDB_PageHandle.h"

void* MyDB_PageHandleBase::getBytes() {
    if (!page->getBufferAddr()) {
       page->setBufferAddr(new char[page->getPageSize()]); 
    }
    return page->getBufferAddr();
}

void MyDB_PageHandleBase::wroteBytes() {
    page->setDirty(true);
}

MyDB_PageHandleBase::MyDB_PageHandleBase(MyDB_Page* page) : page(page) {
    if (!page) {
        cerr << "Error: Null page pointer passed to MyDB_PageHandleBase!" << endl;
        exit(1); 
    }
}

MyDB_PageHandleBase :: ~MyDB_PageHandleBase () {
}

#endif

