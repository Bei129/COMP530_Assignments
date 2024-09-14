#ifndef PAGE_C
#define PAGE_C

#include "MyDB_Page.h"

MyDB_Page::MyDB_Page() {
	this->anonymous = true;
}

// Initialize non-anonymous page
MyDB_Page::MyDB_Page(MyDB_TablePtr whichTable, long i) {
	this->anonymous = false;
	this->pageId.first = whichTable;
	this->pageId.second = i;
}

char* MyDB_Page::getBufferAddr() {
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