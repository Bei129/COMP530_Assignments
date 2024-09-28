#ifndef PAGE_RW_C
#define PAGE_RW_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageHandle.h"
#include <cstring>
#include <MyDB_PageRecIterator.h>

MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_TablePtr myTable, MyDB_BufferManagerPtr myBuffer, size_t pageIndex)
    : myTable(myTable), myBuffer(myBuffer), pageIndex(pageIndex) {
    pageHandle = myBuffer->getPage(myTable, pageIndex);
    pageSize = myBuffer->getPageSize();
}

void MyDB_PageReaderWriter::clear() {
    void* pageData = pageHandle->getBytes();
    memset(pageData, 0, pageSize);  // clear the entire page (set all bytes to 0)
    setType(MyDB_PageType::RegularPage);

    size_t metadataSize = sizeof(MyDB_PageType) + sizeof(size_t);
    size_t* usedSizePtr = (size_t*)((char*)pageData + sizeof(MyDB_PageType));
    *usedSizePtr = metadataSize;

    pageHandle->wroteBytes();   // mark the page as written to (dirty)
}

MyDB_PageType MyDB_PageReaderWriter :: getType () {
	void* pageData = pageHandle->getBytes();
    return *((MyDB_PageType*)pageData);
}

void MyDB_PageReaderWriter::setType(MyDB_PageType toMe) {
    void* pageData = pageHandle->getBytes();
    *((MyDB_PageType*)pageData) = toMe;
    pageHandle->wroteBytes();
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter::getIterator(MyDB_RecordPtr iterateIntoMe) {
    return make_shared<MyDB_PageRecIterator>(iterateIntoMe, *this);
}

bool MyDB_PageReaderWriter::append(MyDB_RecordPtr appendMe) {
    void* pageData = pageHandle->getBytes();
    size_t pageSize = myBuffer->getPageSize();

    size_t* usedSizePtr = (size_t*)((char*)pageData + sizeof(MyDB_PageType));
    size_t usedSize = *usedSizePtr;
    size_t recordSize = appendMe->getBinarySize();  // get the size of the record to append

    // if there is no enough space to add the record, return false
    if (usedSize + recordSize > pageSize) 
        return false;

    appendMe->toBinary((char*)pageData + usedSize);
    *usedSizePtr = usedSize + recordSize;
    pageHandle->wroteBytes();

    return true;
}


#endif
