
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"
#include <unordered_map>

SortMergeJoin :: SortMergeJoin (MyDB_TableReaderWriterPtr leftInputIn, MyDB_TableReaderWriterPtr rightInputIn,
                MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn, 
                vector <string> projectionsIn,
                pair <string, string> equalityCheckIn, string leftSelectionPredicateIn,
                string rightSelectionPredicateIn) {

	output = outputIn;
	finalSelectionPredicate = finalSelectionPredicateIn;
	projections = projectionsIn;
	equalityCheck = equalityCheckIn;
	leftTable = leftInputIn;
	rightTable = rightInputIn; 
	leftSelectionPredicate = leftSelectionPredicateIn;
	rightSelectionPredicate = rightSelectionPredicateIn;
	runSize = leftTable->getBufferMgr ()->numPages / 2;
}

void SortMergeJoin::run() {
    MyDB_RecordPtr leftRecord = leftTable->getEmptyRecord();
    MyDB_RecordPtr tempLeftRecord = leftTable->getEmptyRecord();
    MyDB_RecordPtr rightRecord = rightTable->getEmptyRecord();
    MyDB_RecordPtr tempRightRecord = rightTable->getEmptyRecord();

    auto leftComparator = buildRecordComparator(leftRecord, tempLeftRecord, equalityCheck.first);
    auto rightComparator = buildRecordComparator(rightRecord, tempRightRecord, equalityCheck.second);

    auto leftIter = buildItertorOverSortedRuns(runSize, *leftTable, leftComparator, leftRecord,
                                               tempLeftRecord, leftSelectionPredicate);
    auto rightIter = buildItertorOverSortedRuns(runSize, *rightTable, rightComparator, rightRecord,
                                                tempRightRecord, rightSelectionPredicate);

    auto combinedSchema = std::make_shared<MyDB_Schema>();
    for (const auto& att : leftTable->getTable()->getSchema()->getAtts())
        combinedSchema->appendAtt(att);
    for (const auto& att : rightTable->getTable()->getSchema()->getAtts())
        combinedSchema->appendAtt(att);

    MyDB_RecordPtr combinedRecord = std::make_shared<MyDB_Record>(combinedSchema);

    auto finalPredicate = combinedRecord->compileComputation(finalSelectionPredicate);
    std::vector<func> projectionFunctions;
    for (const auto& proj : projections)
        projectionFunctions.push_back(combinedRecord->compileComputation(proj));

    auto leftSmaller = combinedRecord->compileComputation(" < (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    auto rightSmaller = combinedRecord->compileComputation(" > (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    auto keysEqual = combinedRecord->compileComputation(" == (" + equalityCheck.first + ", " + equalityCheck.second + ")");

    MyDB_RecordPtr outputRecord = output->getEmptyRecord();

    // Initialize buffers for left and right records with the same key
    std::vector<MyDB_PageReaderWriter> leftBufferPages;
    std::vector<MyDB_PageReaderWriter> rightBufferPages;
    MyDB_PageReaderWriter currentLeftPage(true, *leftTable->getBufferMgr());
    MyDB_PageReaderWriter currentRightPage(true, *rightTable->getBufferMgr());

    bool leftHasMore = leftIter->advance();
    bool rightHasMore = rightIter->advance();

    if (!leftHasMore || !rightHasMore)
        return;

    leftIter->getCurrent(leftRecord);
    rightIter->getCurrent(rightRecord);


    while (leftHasMore && rightHasMore) {
        combinedRecord->buildFrom(leftRecord, rightRecord);

        if (leftSmaller()->toBool()) {
            // Advance left iterator
            leftHasMore = leftIter->advance();
            if (leftHasMore)
                leftIter->getCurrent(leftRecord);
        } 
        else if (rightSmaller()->toBool()) {
            // Advance right iterator
            rightHasMore = rightIter->advance();
            if (rightHasMore)
                rightIter->getCurrent(rightRecord);
        } 
        else { // Keys are equal; collect all left and right records with this key

            // Collect left records
            leftBufferPages.clear();
            currentLeftPage.clear();
            leftBufferPages.push_back(currentLeftPage);
            currentLeftPage.append(leftRecord);

            while (true) {
                leftHasMore = leftIter->advance();
                if (!leftHasMore)
                    break;

                leftIter->getCurrent(tempLeftRecord);
                combinedRecord->buildFrom(tempLeftRecord, rightRecord);

                if (keysEqual()->toBool()) {
                    if (!currentLeftPage.append(tempLeftRecord)) {
                        currentLeftPage = MyDB_PageReaderWriter(true, *leftTable->getBufferMgr());
                        leftBufferPages.push_back(currentLeftPage);
                        currentLeftPage.append(tempLeftRecord);
                    }
                } 
                else {
                    // Found a left record with a different key
                    break;
                }
            }

            // Collect right records
            rightBufferPages.clear();
            currentRightPage.clear();
            rightBufferPages.push_back(currentRightPage);
            currentRightPage.append(rightRecord);

            while (true) {
                rightHasMore = rightIter->advance();
                if (!rightHasMore)
                    break;

                rightIter->getCurrent(tempRightRecord);
                combinedRecord->buildFrom(leftRecord, tempRightRecord);

                if (keysEqual()->toBool()) {
                    if (!currentRightPage.append(tempRightRecord)) {
                        currentRightPage = MyDB_PageReaderWriter(true, *rightTable->getBufferMgr());
                        rightBufferPages.push_back(currentRightPage);
                        currentRightPage.append(tempRightRecord);
                    }
                } 
                else { // Found a right record with a different key
                    break;
                }
            }

            // Produce the cross-product of left and right buffers
            auto leftBufferIter = getIteratorAlt(leftBufferPages);
            while (leftBufferIter->advance()) {
                leftBufferIter->getCurrent(leftRecord);

                auto rightBufferIter = getIteratorAlt(rightBufferPages);
                while (rightBufferIter->advance()) {
                    rightBufferIter->getCurrent(rightRecord);

                    combinedRecord->buildFrom(leftRecord, rightRecord);

                    if (finalPredicate()->toBool()) {
                        int i = 0;
                        for (const auto& projFunc : projectionFunctions)
                            outputRecord->getAtt(i++)->set(projFunc());
                        outputRecord->recordContentHasChanged();
                        output->append(outputRecord);
                    }
                }
            }

            // Prepare for next iteration
            if (leftHasMore)
                leftIter->getCurrent(leftRecord);
            if (rightHasMore)
                rightIter->getCurrent(rightRecord);
        }
    }
}


#endif
