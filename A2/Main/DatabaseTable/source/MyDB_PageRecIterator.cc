#include "MyDB_PageRecIterator.h"
#include "MyDB_BufferManager.h"

MyDB_PageRecIterator::MyDB_PageRecIterator(MyDB_RecordPtr iterateIntoMe, MyDB_PageReaderWriter& parentPage)
    : iterateIntoMe(iterateIntoMe), parentPage(parentPage) {
    MyDB_PageHandle pageHandle = parentPage.pageHandle;
    pageData = (char*)pageHandle->getBytes();
    pageSize = parentPage.myBuffer->getPageSize();

    size_t* usedSizePtr = (size_t*)(pageData + sizeof(MyDB_PageType));
    usedSize = *usedSizePtr;

    // initialize offsets, skip metadata fields for page type and used space size
    offset = sizeof(MyDB_PageType) + sizeof(size_t);
}

void MyDB_PageRecIterator::getNext() {
    if (!hasNext()) 
        return;

    char* currentPtr = pageData + offset;
    currentPtr = (char*)iterateIntoMe->fromBinary(currentPtr);
    offset = currentPtr - pageData;
}

bool MyDB_PageRecIterator::hasNext() {
    return offset < usedSize;
}
