#ifndef TABLE_REC_ITERATOR_H
#define TABLE_REC_ITERATOR_H

#include "MyDB_RecordIterator.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_Record.h"
#include "MyDB_PageRecIterator.h"

class MyDB_TableRecIterator : public MyDB_RecordIterator {
    
public:
    MyDB_TableRecIterator(MyDB_RecordPtr iterateIntoMe, MyDB_TableReaderWriter& parentTable);

    void getNext() override;
    bool hasNext() override;

private:
    MyDB_RecordPtr iterateIntoMe;
    MyDB_TableReaderWriter& parentTable;
    size_t currentPageIndex;
    size_t totalPages;
    MyDB_RecordIteratorPtr pageIter;
};

#endif
