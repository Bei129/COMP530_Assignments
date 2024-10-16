#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return getRangeIteratorAltHelper(low, high, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return getRangeIteratorAltHelper(low, high, false);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter::getRangeIteratorAltHelper(MyDB_AttValPtr lowKey, MyDB_AttValPtr highKey, bool isSorted) {
    vector<MyDB_PageReaderWriter> pages;
    
    // Discover and collect all pages that fall within the range [lowKey, highKey]
    discoverPages(this->rootLocation, pages, lowKey, highKey);

    MyDB_RecordPtr lhsRecord = getEmptyRecord();
    MyDB_RecordPtr rhsRecord = getEmptyRecord();
    MyDB_RecordPtr currentRecord = getEmptyRecord();

    MyDB_INRecordPtr lowBoundRecord = getINRecord();
    MyDB_INRecordPtr highBoundRecord = getINRecord();

    lowBoundRecord->setKey(lowKey);
    highBoundRecord->setKey(highKey);

    function<bool()> mainComparator = buildComparator(lhsRecord, rhsRecord); 
    function<bool()> lowBoundComparator = buildComparator(currentRecord, lowBoundRecord); 
    function<bool()> highBoundComparator = buildComparator(highBoundRecord, currentRecord);

    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages, lhsRecord, rhsRecord, mainComparator, currentRecord, lowBoundComparator, highBoundComparator, isSorted);
}


bool MyDB_BPlusTreeReaderWriter::discoverPages(int whichPage, vector<MyDB_PageReaderWriter> &list, MyDB_AttValPtr lowKey, MyDB_AttValPtr highKey) {
    MyDB_PageReaderWriter page = (*this)[whichPage];

    // If this is a leaf page, add it to the list and return
    if (page.getType() == MyDB_PageType::RegularPage) {
        list.push_back(page);
        return true;
    }

    MyDB_RecordIteratorAltPtr pageIterator = page.getIteratorAlt();
    MyDB_INRecordPtr currentRecord = getINRecord();  
    MyDB_INRecordPtr lowRecord = getINRecord(); 
    MyDB_INRecordPtr highRecord = getINRecord(); 

    lowRecord->setKey(lowKey);
    highRecord->setKey(highKey);

    function<bool()> compareLow = buildComparator(currentRecord, lowRecord);
    function<bool()> compareHigh = buildComparator(highRecord, currentRecord);

    bool foundRangeStart = false;

    // Iterate through the internal records and recursively discover pages
    while (pageIterator->advance()) {
        pageIterator->getCurrent(currentRecord);

        if (!compareLow()) {
            foundRangeStart = true;  
        }

        if (foundRangeStart) {
            discoverPages(currentRecord->getPtr(), list, lowKey, highKey);
        }

        if (compareHigh()) {
            break;
        }
    }

    return false;
}


void MyDB_BPlusTreeReaderWriter::append(MyDB_RecordPtr rec) {
    if (rootLocation == -1) {
        rootLocation++;
        MyDB_PageReaderWriter rootPage = (*this)[rootLocation];
        rootPage.setType(MyDB_PageType::DirectoryPage);

        // Create a new leaf page and set it as the child of the root
        int leafPageLoc = getTable()->lastPage() + 1;
        getTable()->setLastPage(leafPageLoc);
        MyDB_PageReaderWriter leafPage = (*this)[leafPageLoc];
        leafPage.clear();
        leafPage.setType(MyDB_PageType::RegularPage);

        // Create an internal node that points to the new leaf page
        MyDB_INRecordPtr rootPtr = getINRecord();
        rootPtr->setPtr(leafPageLoc);
        rootPage.append(rootPtr);
    }

    MyDB_RecordPtr splitResult = append(rootLocation, rec);

    if (splitResult != nullptr) {
        int newRootLoc = getTable()->lastPage() + 1;
        getTable()->setLastPage(newRootLoc);
        MyDB_PageReaderWriter newRootPage = (*this)[newRootLoc];
        newRootPage.clear();
        newRootPage.setType(MyDB_PageType::DirectoryPage);

        // Create an internal node that points to the original root
        MyDB_INRecordPtr rootPtr = getINRecord();
        rootPtr->setPtr(rootLocation);

        // Add the new split result and the original root to the new root page
        newRootPage.append(splitResult);
        newRootPage.append(rootPtr);

        rootLocation = newRootLoc;
    }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter::split(MyDB_PageReaderWriter splitMe, MyDB_RecordPtr addMe) {
    MyDB_INRecordPtr newPtr = getINRecord();

    vector<MyDB_RecordPtr> records;
    MyDB_RecordIteratorAltPtr recordIter = splitMe.getIteratorAlt();
    MyDB_INRecordPtr currentRecord = getINRecord();

    while (recordIter->advance()) {
        recordIter->getCurrent(currentRecord);
        MyDB_INRecordPtr recordCopy = getINRecord();
        recordCopy->setKey(currentRecord->getKey());
        recordCopy->setPtr(currentRecord->getPtr());
        records.push_back(recordCopy);
    }
    records.push_back(addMe);

    // Sort the records once
    MyDB_RecordPtr tempRec1 = getEmptyRecord();
    MyDB_RecordPtr tempRec2 = getEmptyRecord();
    function<bool()> myComparator = buildComparator(tempRec1, tempRec2);
	// !! might cause errors
    std::sort(records.begin(), records.end(), [myComparator](MyDB_RecordPtr a, MyDB_RecordPtr b) {
        return myComparator();
    });

    size_t mid = records.size() / 2 - 1;

    // Prepare new pages
    int leftPageLoc = getTable()->lastPage() + 1;
    getTable()->setLastPage(leftPageLoc);
    MyDB_PageReaderWriter leftPage = (*this)[leftPageLoc];
    leftPage.clear();

    int rightPageLoc = getTable()->lastPage() + 1;
    getTable()->setLastPage(rightPageLoc);
    MyDB_PageReaderWriter rightPage = (*this)[rightPageLoc];
    rightPage.clear();

    // Distribute records between left and right pages
    for (size_t i = 0; i < mid; i++) {
        leftPage.append(records[i]);
    }
    for (size_t i = mid; i < records.size(); i++) {
        rightPage.append(records[i]);
    }

    // Set the new pointer for the middle key 
    newPtr->setPtr(rightPageLoc);
    newPtr->setKey(getKey(records[mid]));

    // Copy records from rightPage back to splitMe
    splitMe.clear();
    recordIter = rightPage.getIteratorAlt();
    while (recordIter->advance()) {
        recordIter->getCurrent(currentRecord);
        splitMe.append(currentRecord);
    }

    return newPtr;
}


bool MyDB_BPlusTreeReaderWriter::insertInSortedOrder(MyDB_PageReaderWriter& page, MyDB_RecordPtr rec) {
    vector<MyDB_RecordPtr> records;

    // Collect existing records from the page
    MyDB_RecordIteratorAltPtr iter = page.getIteratorAlt();
    MyDB_INRecordPtr temp = getINRecord();
    function<bool()> comparator = buildComparator(rec, temp);

    bool inserted = false;
    while (iter->advance()) {
        iter->getCurrent(temp);
        
        MyDB_INRecordPtr tempCopy = getINRecord();
        tempCopy->setKey(temp->getKey());
        tempCopy->setPtr(temp->getPtr());
        
        if (!inserted && comparator()) {
            records.push_back(rec);
            inserted = true;
        }

        records.push_back(tempCopy);
    }

    // If the new record wasn't inserted, append it to the end
    if (!inserted) {
        records.push_back(rec);
    }

    page.clear();
    for (const auto& record : records) {
        page.append(record);
    }

    return true;
}


MyDB_RecordPtr MyDB_BPlusTreeReaderWriter::append(int whichPage, MyDB_RecordPtr appendMe) {
    MyDB_PageReaderWriter page = (*this)[whichPage];

    if (appendMe->getSchema() == nullptr) { 
		if (!insertInSortedOrder(page, appendMe)) {
			return split(page, appendMe);
		}
	}
	else { // appendMe is a leaf record
        if (page.getType() == MyDB_PageType::RegularPage) {
            if (page.append(appendMe)) {
                return nullptr;
            } 
			else { // Page is full, need to split
                MyDB_RecordPtr splitResult = split(page, appendMe);
                return splitResult;
            }
        } 
		else {
            MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();
            MyDB_INRecordPtr currentRecord = getINRecord();

            while (recordIter->advance()) {
                recordIter->getCurrent(currentRecord);
                function<bool()> myComparator = buildComparator(appendMe, currentRecord);
                if (myComparator()) {
                    MyDB_RecordPtr result = append(currentRecord->getPtr(), appendMe);
                    if (result == nullptr) {
                        return nullptr;
                    } else {
                        if (page.append(result)) {
                            // !! might wrong (Sort the page after insertion
                            MyDB_INRecordPtr tempRec1 = getINRecord();
                            MyDB_INRecordPtr tempRec2 = getINRecord();
                            function<bool()> myComparator = buildComparator(tempRec1, tempRec2);
                            page.sortInPlace(myComparator, tempRec1, tempRec2);
                            return nullptr;
                        } else {
                            MyDB_RecordPtr splitResult = split(page, result);
                            return splitResult;
                        }
                    }
                }
            }

            // If appendMe is greater than all keys, recurse into the last child
            recordIter = page.getIteratorAlt();
            while (recordIter->advance()) {
                recordIter->getCurrent(currentRecord);
            }
            // Recursively insert into the last child
            MyDB_RecordPtr result = append(currentRecord->getPtr(), appendMe);
            if (result == nullptr) {
                return nullptr;
            } 
			else {
                // Child node was split, need to handle the split
                if (page.append(result)) {
                    // !! might wrong: Sort the page after insertion
                    MyDB_INRecordPtr tempRec1 = getINRecord();
                    MyDB_INRecordPtr tempRec2 = getINRecord();
                    function<bool()> myComparator = buildComparator(tempRec1, tempRec2);
                    page.sortInPlace(myComparator, tempRec1, tempRec2);
                    return nullptr;
                } else {
                    MyDB_RecordPtr splitResult = split(page, result);
                    return splitResult;
                }
            }
        }
    }
}



MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
	printHelper(this->rootLocation, 0);
}

void MyDB_BPlusTreeReaderWriter :: printHelper (int whichPage, int depth) {
    MyDB_PageReaderWriter page = (*this)[whichPage];
    MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();

    if (page.getType() == MyDB_PageType::RegularPage) {
        MyDB_RecordPtr record = getEmptyRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            cout << string(depth, '\t') << record << "\n";
        }
    } 
	else {
        MyDB_INRecordPtr inRecord = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(inRecord);
            printHelper(inRecord->getPtr(), depth + 1);
            cout << string(depth, '\t') << inRecord->getKey() << "\n";
        }
    }
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

#endif
