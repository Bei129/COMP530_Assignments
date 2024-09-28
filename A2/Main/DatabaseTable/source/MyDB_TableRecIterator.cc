#include "MyDB_TableRecIterator.h"

MyDB_TableRecIterator::MyDB_TableRecIterator(MyDB_RecordPtr iterateIntoMe, MyDB_TableReaderWriter& parentTable)
    : iterateIntoMe(iterateIntoMe), parentTable(parentTable) {
    currentPageIndex = 0;
    totalPages = parentTable.myTable->lastPage(); 

    // if the table has pages, get the iterator for the first page
    if (totalPages > 0) {
        MyDB_PageReaderWriter currentPage = parentTable[currentPageIndex];
        pageIter = currentPage.getIterator(iterateIntoMe);
    }
}

void MyDB_TableRecIterator::getNext() {
    if (!hasNext()) 
        return;

    if (pageIter->hasNext()) // get the next record if the current page iteraotr has more records
        pageIter->getNext();
    else {
        currentPageIndex++;
        while (currentPageIndex < totalPages) {
            MyDB_PageReaderWriter currentPage = parentTable[currentPageIndex];
            pageIter = currentPage.getIterator(iterateIntoMe);
            if (pageIter->hasNext()) {
                pageIter->getNext();
                return;
            }
            currentPageIndex++;
        }
    }
}

bool MyDB_TableRecIterator::hasNext() {
    if (pageIter && pageIter->hasNext()) 
        return true;
    else { // look ahead to the next page if the current is exhausted
        size_t tempPageIndex = currentPageIndex + 1;
        while (tempPageIndex < totalPages) {
            MyDB_PageReaderWriter tempPage = parentTable[tempPageIndex];
            MyDB_RecordIteratorPtr tempIter = tempPage.getIterator(iterateIntoMe);
            if (tempIter->hasNext()) 
                return true;
            tempPageIndex++;
        }
        return false;
    }
}

