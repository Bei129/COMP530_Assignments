#ifndef PAGE_REC_ITERATOR_H
#define PAGE_REC_ITERATOR_H

#include "MyDB_RecordIterator.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_Record.h"
#include <memory>

class MyDB_PageRecIterator : public MyDB_RecordIterator {
    
public:
    MyDB_PageRecIterator(MyDB_RecordPtr iterateIntoMe, MyDB_PageReaderWriter& parentPage);

    void getNext() override;
    bool hasNext() override;

private:
    MyDB_RecordPtr iterateIntoMe;   // store where the next record is placed
    MyDB_PageReaderWriter& parentPage;  // the page the iterator is traversing
    char* pageData;
    size_t offset;
    size_t usedSize;
    size_t pageSize;
};

#endif
