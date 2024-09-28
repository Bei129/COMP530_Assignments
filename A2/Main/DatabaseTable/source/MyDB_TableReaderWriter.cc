#ifndef TABLE_RW_C
#define TABLE_RW_C

#include <fstream>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_PageRecIterator.h"
#include "MyDB_TableRecIterator.h"

using namespace std;

MyDB_TableReaderWriter::MyDB_TableReaderWriter(MyDB_TablePtr forMe, MyDB_BufferManagerPtr myBuffer)
    : myTable(forMe), myBuffer(myBuffer) {
}

MyDB_PageReaderWriter MyDB_TableReaderWriter :: operator [] (size_t i) {
    while (i > myTable->lastPage()) {
        myTable->setLastPage(myTable->lastPage()+1);
        shared_ptr <MyDB_PageReaderWriter> lastPage = make_shared <MyDB_PageReaderWriter>(myTable, myBuffer, myTable->lastPage());
        lastPage->clear();
    }
	return MyDB_PageReaderWriter(myTable, myBuffer, i);
}

MyDB_RecordPtr MyDB_TableReaderWriter::getEmptyRecord() {
    return make_shared<MyDB_Record>(myTable->getSchema());
}

MyDB_PageReaderWriter MyDB_TableReaderWriter::last() {
    size_t totalPages = myTable->lastPage();
    if (totalPages == 0) {
        myTable->setLastPage(1);    // create the first page if there are no pages
        MyDB_PageReaderWriter newPage = (*this)[0];
        newPage.clear(); // clear the new page
        return newPage;
    } 
    else {
        return (*this)[totalPages - 1];
    }
}

void MyDB_TableReaderWriter::append(MyDB_RecordPtr appendMe) {
    size_t totalPages = myTable->lastPage();
    size_t lastPageIndex;
    MyDB_PageReaderWriter lastPage = (*this)[0]; // temporary initialization

    if (totalPages == 0) {  // if the table is empty, create the first page
        lastPageIndex = 0;
        myTable->setLastPage(1); 
        lastPage = (*this)[lastPageIndex];
        lastPage.clear();
    } 
    else {  // table not empty, get last page
        lastPageIndex = totalPages - 1;
        lastPage = (*this)[lastPageIndex];
    }

    if (!lastPage.append(appendMe)) {   // create a new page if the current is full
        lastPageIndex = totalPages; 
        myTable->setLastPage(lastPageIndex + 1); // update the total page number

        lastPage = (*this)[lastPageIndex];
        lastPage.clear(); 

        if (!lastPage.append(appendMe)) 
            cerr << "Failed to append record to a new page." << endl;
    }
}


void MyDB_TableReaderWriter::loadFromTextFile(string fromMe) {
    myTable->setLastPage(0);    // reset the table

    ifstream infile(fromMe);
    if (!infile.is_open()) {
        cerr << "Failed to open file: " << fromMe << endl;
        return;
    }

    MyDB_RecordPtr tempRec = getEmptyRecord();
    string line;
    while (getline(infile, line)) {
        tempRec->fromString(line);  // convert the line to a record
        append(tempRec);
    }

    infile.close();
}


MyDB_RecordIteratorPtr MyDB_TableReaderWriter::getIterator(MyDB_RecordPtr iterateIntoMe) {
    return make_shared<MyDB_TableRecIterator>(iterateIntoMe, *this);
}

void MyDB_TableReaderWriter::writeIntoTextFile(string toMe) {
    ofstream outfile(toMe);
    if (!outfile.is_open()) {
        cerr << "Failed to open file: " << toMe << endl;
        return;
    }
    MyDB_RecordPtr tempRec = getEmptyRecord();
    MyDB_RecordIteratorPtr iter = getIterator(tempRec);

    while (iter->hasNext()) {
        iter->getNext();
        outfile << tempRec << endl;
    }

    outfile.close();
}

#endif

