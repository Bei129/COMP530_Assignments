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

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAltHelper(MyDB_AttValPtr lowKey, MyDB_AttValPtr highKey, bool isSorted) {
    vector<MyDB_PageReaderWriter> pages;
    discoverPages(this->rootLocation, pages, lowKey, highKey);

    MyDB_RecordPtr currentRecord = getEmptyRecord();
    MyDB_INRecordPtr lowBoundRecord = getINRecord();
    MyDB_INRecordPtr highBoundRecord = getINRecord();

    lowBoundRecord->setKey(lowKey);
    highBoundRecord->setKey(highKey);

    function<bool()> lowBoundComparator = buildComparator(currentRecord, lowBoundRecord); 
    function<bool()> highBoundComparator = buildComparator(highBoundRecord, currentRecord);

    // Only create the main comparator if sorting is required
    function<bool()> mainComparator = isSorted ? buildComparator(currentRecord, currentRecord) : nullptr;

    // Create and return the iterator, passing the necessary parameters
    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(
        pages, 
        getEmptyRecord(),  // lhsRecord
        getEmptyRecord(),  // rhsRecord
        mainComparator,    // Only used if isSorted is true
        currentRecord, 
        lowBoundComparator, 
        highBoundComparator, 
        isSorted
    );
}

bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector<MyDB_PageReaderWriter> &list, MyDB_AttValPtr lowKey, MyDB_AttValPtr highKey) {
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

        int leafPageLoc = getTable()->lastPage() + 1;
        getTable()->setLastPage(leafPageLoc);
        MyDB_PageReaderWriter leafPage = (*this)[leafPageLoc];
        leafPage.clear();
        leafPage.setType(MyDB_PageType::RegularPage);

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

        MyDB_INRecordPtr rootPtr = getINRecord();
        rootPtr->setPtr(rootLocation);
        newRootPage.append(splitResult);
        newRootPage.append(rootPtr);

        rootLocation = newRootLoc;
    }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
    MyDB_INRecordPtr newPtr = getINRecord();

    int leftPageLoc = getTable()->lastPage() + 1;
    getTable()->setLastPage(leftPageLoc);
    MyDB_PageReaderWriter leftPage = (*this)[leftPageLoc];
    leftPage.clear();

    int rightPageLoc = getTable()->lastPage() + 1;
    MyDB_PageReaderWriter rightPage = (*this)[rightPageLoc];
    rightPage.clear();

    // Check if we are splitting a leaf node (RegularPage)
    if (splitMe.getType() == MyDB_PageType::RegularPage) {
        leftPage.setType(MyDB_PageType::RegularPage);
        rightPage.setType(MyDB_PageType::RegularPage);

        MyDB_RecordPtr tempRec1 = getEmptyRecord();
        MyDB_RecordPtr tempRec2 = getEmptyRecord();
        function<bool()> myComparator = buildComparator(tempRec1, tempRec2);

        splitMe.sortInPlace(myComparator, tempRec1, tempRec2);

        MyDB_RecordIteratorAltPtr recordIter = splitMe.getIteratorAlt();
        int totalRecords = 0;
        MyDB_RecordPtr currentRecord = getEmptyRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(currentRecord);
            totalRecords++;
        }

        // Calculate the middle index for splitting
        int midIndex = totalRecords / 2 - 1;
        if (midIndex < 0) midIndex = 0;

        // Reset the iterator to distribute records between leftPage and rightPage
        recordIter = splitMe.getIteratorAlt();
        currentRecord = getEmptyRecord();
        int count = 0;
        bool newPtrSet = false;

        while (recordIter->advance()) {
            recordIter->getCurrent(currentRecord);

            // Set newPtr with the key and pointer at the middle record
            if (count == midIndex && !newPtrSet) {
                newPtrSet = true;
                newPtr->setPtr(leftPageLoc);
                newPtr->setKey(getKey(currentRecord));
            }

            // Distribute records to leftPage and rightPage
            if (count <= midIndex) {
                leftPage.append(currentRecord);
            } else {
                rightPage.append(currentRecord);
            }
            count++;
        }

        // Decide where to insert andMe based on the comparator
        function<bool()> compareAddMe = buildComparator(andMe, newPtr);
        if (compareAddMe()) {
            leftPage.append(andMe);
            leftPage.sortInPlace(myComparator, tempRec1, tempRec2);
        } else {
            rightPage.append(andMe);
            rightPage.sortInPlace(myComparator, tempRec1, tempRec2);
        }

        // Copy records from rightPage back to splitMe (which becomes the right page)
        splitMe.clear();
        recordIter = rightPage.getIteratorAlt();
        currentRecord = getEmptyRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(currentRecord);
            splitMe.append(currentRecord);
        }
        rightPage.clear();

    } 
	else { // Splitting an internal node (DirectoryPage)
        leftPage.setType(MyDB_PageType::DirectoryPage);
        rightPage.setType(MyDB_PageType::DirectoryPage);

        MyDB_INRecordPtr tempRec1 = getINRecord();
        MyDB_INRecordPtr tempRec2 = getINRecord();
        function<bool()> myComparator = buildComparator(tempRec1, tempRec2);

        splitMe.sortInPlace(myComparator, tempRec1, tempRec2);

        // Count the number of records in splitMe
        MyDB_RecordIteratorAltPtr recordIter = splitMe.getIteratorAlt();
        int totalRecords = 0;
        MyDB_INRecordPtr currentRecord = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(currentRecord);
            totalRecords++;
        }

        int midIndex = totalRecords / 2 - 1;
        if (midIndex < 0) midIndex = 0;

        // Reset the iterator to distribute records between leftPage and rightPage
        recordIter = splitMe.getIteratorAlt();
        currentRecord = getINRecord();
        int count = 0;
        bool newPtrSet = false;

        while (recordIter->advance()) {
            recordIter->getCurrent(currentRecord);

            // Set newPtr with the key and pointer at the middle record
            if (count == midIndex && !newPtrSet) {
                newPtrSet = true;
                newPtr->setPtr(leftPageLoc);
                newPtr->setKey(getKey(currentRecord));
            }

            // Distribute records to leftPage and rightPage
            if (count <= midIndex) {
                leftPage.append(currentRecord);
            } 
			else {
                rightPage.append(currentRecord);
            }
            count++;
        }

        // Decide where to insert andMe based on the comparator
        function<bool()> compareAddMe = buildComparator(andMe, newPtr);
        if (compareAddMe()) {
            leftPage.append(andMe);
            leftPage.sortInPlace(myComparator, tempRec1, tempRec2);
        } 
		else {
            rightPage.append(andMe);
            rightPage.sortInPlace(myComparator, tempRec1, tempRec2);
        }

        // Copy records from rightPage back to splitMe (which becomes the right page)
        splitMe.clear();
        splitMe.setType(MyDB_PageType::DirectoryPage);
        recordIter = rightPage.getIteratorAlt();
        currentRecord = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(currentRecord);
            splitMe.append(currentRecord);
        }
        rightPage.clear();
    }
    return newPtr;
}


MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
    MyDB_PageReaderWriter page = (*this)[whichPage];

    if (appendMe->getSchema() == nullptr) {
        if (page.append(appendMe)) {
            // Sort the page after insertion
            MyDB_INRecordPtr tempRec1 = getINRecord();
            MyDB_INRecordPtr tempRec2 = getINRecord();
            function<bool()> myComparator = buildComparator(tempRec1, tempRec2);
            page.sortInPlace(myComparator, tempRec1, tempRec2);
            return nullptr;
        } 
		else { // Page is full, need to split
            MyDB_RecordPtr splitResult = split(page, appendMe);
            return splitResult;
        }
    } 
	else { // appendMe is a leaf record
        if (page.getType() == MyDB_PageType::RegularPage) {
            if (page.append(appendMe)) {
                return nullptr;
            } 
			else {
                // Page is full, need to split
                MyDB_RecordPtr splitResult = split(page, appendMe);
                return splitResult;
            }
        } 
		else { // Current page is an internal node, need to find the correct child
            MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();
            MyDB_INRecordPtr currentRecord = getINRecord();

            while (recordIter->advance()) {
                recordIter->getCurrent(currentRecord);
                function<bool()> myComparator = buildComparator(appendMe, currentRecord);
                if (myComparator()) {
                    MyDB_RecordPtr result = append(currentRecord->getPtr(), appendMe);
                    if (result == nullptr) {
                        return nullptr;
                    } 
					else { // Child node was split, need to handle the split
                        if (page.append(result)) {
                            // Sort the page after insertion
                            MyDB_INRecordPtr tempRec1 = getINRecord();
                            MyDB_INRecordPtr tempRec2 = getINRecord();
                            function<bool()> myComparator = buildComparator(tempRec1, tempRec2);
                            page.sortInPlace(myComparator, tempRec1, tempRec2);
                            return nullptr;
                        } 
						else { // Current page is full, need to split
                            MyDB_RecordPtr splitResult = split(page, result);
                            return splitResult;
                        }
                    }
                }
            }

            // If appendMe is greater than all keys, recurse into the last child
            // Reset iterator to get the last child
            recordIter = page.getIteratorAlt();
            while (recordIter->advance()) {
                recordIter->getCurrent(currentRecord);
            }
            // Recursively insert into the last child
            MyDB_RecordPtr result = append(currentRecord->getPtr(), appendMe);
            if (result == nullptr) {
                // Insertion was successful without splitting
                return nullptr;
            } 
			else { // Child node was split, need to handle the split
                if (page.append(result)) {
                    // Sort the page after insertion
                    MyDB_INRecordPtr tempRec1 = getINRecord();
                    MyDB_INRecordPtr tempRec2 = getINRecord();
                    function<bool()> myComparator = buildComparator(tempRec1, tempRec2);
                    page.sortInPlace(myComparator, tempRec1, tempRec2);
                    return nullptr;
                } 
				else { // Current page is full, need to split
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

void MyDB_BPlusTreeReaderWriter :: printTree() {
    int rootPage = this->rootLocation;
    int depth = 0;
    MyDB_PageReaderWriter page = (*this)[rootPage];
    MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();

    // If this is a leaf node
    if (page.getType() == MyDB_PageType::RegularPage) {
        MyDB_RecordPtr record = getEmptyRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            cout << string(depth, '\t') << record << "\n";
        }
    } 
    else { // Otherwise, this is an internal node (DirectoryPage)
        MyDB_INRecordPtr inRecord = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(inRecord);
            int childPage = inRecord->getPtr();

            MyDB_PageReaderWriter childPageRW = (*this)[childPage];
            MyDB_RecordIteratorAltPtr childIter = childPageRW.getIteratorAlt();

            if (childPageRW.getType() == MyDB_PageType::RegularPage) {
                MyDB_RecordPtr childRecord = getEmptyRecord();
                while (childIter->advance()) {
                    childIter->getCurrent(childRecord);
                    cout << string(depth + 1, '\t') << childRecord << "\n";
                }
            } 
			else {
                while (childIter->advance()) {
                    childIter->getCurrent(inRecord);
                    cout << string(depth + 1, '\t') << inRecord->getKey() << "\n";
                }
            }

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
