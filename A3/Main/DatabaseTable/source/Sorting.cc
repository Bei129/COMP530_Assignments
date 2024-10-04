
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "RecordComparator.h"
#include "Sorting.h"

using namespace std;

void mergeIntoFile(MyDB_TableReaderWriter& sortIntoMe, vector <MyDB_RecordIteratorAltPtr>& mergeUs,
	function <bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	auto comparator2 = [comparator, lhs, rhs](MyDB_RecordIteratorAltPtr l, MyDB_RecordIteratorAltPtr r) {
		RecordComparator recordComparator(comparator, lhs, rhs);
		return !recordComparator(l->getCurrentPointer(), r->getCurrentPointer());
	};
	priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, decltype(comparator2)> pq(comparator2, mergeUs);
	MyDB_RecordIteratorAltPtr tempPtr;
	while (!pq.empty()) {
		tempPtr = pq.top();
		pq.pop();

		tempPtr->getCurrent(lhs);
		sortIntoMe.append(lhs);

		if (tempPtr->advance()) {
			pq.push(tempPtr);
		}
	}
}

vector <MyDB_PageReaderWriter> mergeIntoList(MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter,
	MyDB_RecordIteratorAltPtr rightIter, function <bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	vector<MyDB_PageReaderWriter> vec;
	MyDB_PageReaderWriter curPage(*parent);
	bool leftFlag = leftIter->advance();
	bool rightFlag = rightIter->advance();
	
	while (leftFlag && rightFlag) {
		leftIter->getCurrent(lhs);
		rightIter->getCurrent(rhs);
		if (comparator()) {
			
			if (!curPage.append(lhs)) {
				vec.push_back(curPage);
				MyDB_PageReaderWriter newPage(*parent);
				curPage = newPage;
				curPage.append(lhs);
			}
			leftFlag = leftIter->advance();
		}
		else {
			
			if (!curPage.append(rhs)) {
				vec.push_back(curPage);
				MyDB_PageReaderWriter newPage(*parent);
				curPage = newPage;
				curPage.append(rhs);
			}
			rightFlag = rightIter->advance();
		}
	}

	while (leftFlag) {
		leftIter->getCurrent(lhs);
		if (!curPage.append(lhs)) {
			vec.push_back(curPage);
			MyDB_PageReaderWriter newPage(*parent);
			curPage = newPage;
			curPage.append(lhs);
		}
		leftFlag = leftIter->advance();
	}

	while (rightFlag) {
		rightIter->getCurrent(rhs);
		if (!curPage.append(rhs)) {
			vec.push_back(curPage);
			MyDB_PageReaderWriter newPage(*parent);
			curPage = newPage;
			curPage.append(rhs);
		}
		rightFlag = rightIter->advance();
	}

	vec.push_back(curPage);

	return vec; 
}
	
void sort(int runSize, MyDB_TableReaderWriter& sortMe, MyDB_TableReaderWriter& sortIntoMe,
	function <bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	vector<MyDB_RecordIteratorAltPtr> mergeUs;
	for (int i = 0; i < sortMe.getNumPages(); i+=runSize) {
		vector<vector<MyDB_PageReaderWriter>> mergeLists;
		vector<MyDB_PageReaderWriter> tempPage;
		for (int j = i; j < sortMe.getNumPages() && j < i + runSize; j++) {
			tempPage.push_back(*sortMe[j].sort(comparator, lhs, rhs));
			mergeLists.push_back(tempPage);
			tempPage.clear();
		}
		while (mergeLists.size() > 1) {
			mergeLists.push_back(mergeIntoList(sortMe.getBufferMgr(), getIteratorAlt(mergeLists[0]), getIteratorAlt(mergeLists[1]), comparator, lhs, rhs));
			mergeLists.erase(mergeLists.begin(), mergeLists.begin() + 2);
		}
		if (mergeLists.size() == 1) {
			mergeUs.push_back(getIteratorAlt(mergeLists[0]));
		}
	}
	mergeIntoFile(sortIntoMe, mergeUs, comparator, lhs, rhs);
}

#endif
