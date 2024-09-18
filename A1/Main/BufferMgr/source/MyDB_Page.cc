#ifndef PAGE_C
#define PAGE_C

#include "MyDB_Page.h"
#include <iostream>

MyDB_Page::MyDB_Page(size_t pageSize)  {
	this->anonymous = true;
	this->pageSize = pageSize;
	this->bufferAddr = new char[pageSize];  
}

// Initialize non-anonymous page
MyDB_Page::MyDB_Page(MyDB_TablePtr whichTable, long i, size_t pageSize) {
    this->anonymous = false;
    this->pageId.first = whichTable;
    this->pageId.second = i;
    this->pageSize = pageSize;
    this->bufferAddr = new char[pageSize]; 
}

char* MyDB_Page::getBufferAddr() {
	if (!bufferAddr) {
        cerr << "Error: Buffer address is not initialized!" << endl;
        exit(1); 
    }
	return bufferAddr;
}
void MyDB_Page::setBufferAddr(char* bufferAddr) {
	this->bufferAddr = bufferAddr;
}

pair<MyDB_TablePtr, long> MyDB_Page::getPageId() {
	return pageId;
}

int MyDB_Page::getSlotId() {
	return slotId;
}
void MyDB_Page::setSlotId(int slotId){
	this->slotId = slotId;
}

bool MyDB_Page::isAnonymous() {
	return anonymous;
}

bool MyDB_Page::isPinned() {
	return pinned;
}
void MyDB_Page::setPinned() {
	this->pinned = true;
}
void MyDB_Page::undoPinned() {
	this->pinned = false;
}

bool MyDB_Page::isDirty() {
	return dirty;
}
void MyDB_Page::setDirty(bool dirty) {
	this->dirty = dirty;
}

#endif