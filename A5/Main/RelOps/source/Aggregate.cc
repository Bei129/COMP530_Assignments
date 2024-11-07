
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>

using namespace std;

Aggregate::Aggregate(MyDB_TableReaderWriterPtr inputTable,
                     MyDB_TableReaderWriterPtr outputTable,
                     vector<pair<MyDB_AggType, string>> aggsToCompute,
                     vector<string> groupings,
                     string selectionPredicate)
    : input(inputTable), output(outputTable), aggsToCompute(aggsToCompute), groupings(groupings), selectionPredicate(selectionPredicate) {}

void Aggregate::run() {
    int numOutputAtts = output->getTable()->getSchema()->getAtts().size();
    int expectedAtts = aggsToCompute.size() + groupings.size();
    if (numOutputAtts != expectedAtts) {
        cout << "Error: Output schema must match the number of aggregations and groups.\n";
        return;
    }

    // Initialize schemas for aggregate and combined records
    MyDB_SchemaPtr aggSchema = make_shared<MyDB_Schema>();
    MyDB_SchemaPtr combinedSchema = make_shared<MyDB_Schema>();
    
    int groupCount = groupings.size();
    int i = 0;

    for (auto& att : output->getTable()->getSchema()->getAtts()) {
        if (i < groupCount)
            aggSchema->appendAtt(make_pair("MyDB_GroupAtt" + to_string(i++), att.second));
        else
            aggSchema->appendAtt(make_pair("MyDB_AggAtt" + to_string(i++ - groupCount), att.second));
    }
    aggSchema->appendAtt(make_pair("MyDB_CntAtt", make_shared<MyDB_IntAttType>()));

    for (auto& att : input->getTable()->getSchema()->getAtts())
        combinedSchema->appendAtt(att);
    for (auto& att : aggSchema->getAtts())
        combinedSchema->appendAtt(att);

    // Initialize records and pages
    MyDB_RecordPtr inputRecord = input->getEmptyRecord();
    MyDB_RecordPtr aggRecord = make_shared<MyDB_Record>(aggSchema);
    MyDB_RecordPtr combinedRecord = make_shared<MyDB_Record>(combinedSchema);
    combinedRecord->buildFrom(inputRecord, aggRecord);
    MyDB_PageReaderWriter currentAggregatePage(true, *(input->getBufferMgr()));
    vector<MyDB_PageReaderWriter> aggregatePages;
    aggregatePages.push_back(currentAggregatePage);

    // Create hash table for aggregate records
    unordered_map<size_t, vector<void*>> aggregateHashTable;

    // Prepare grouping computations and match function
    vector<func> groupingComputations;
    for (auto& expr : groupings) {
        groupingComputations.push_back(inputRecord->compileComputation(expr));
    }
    
    func groupMatchFunction;
    string groupMatchExpr = "bool[true]";
    i = 0;
    for (auto& expr : groupings) {
        string clause = "== (" + expr + ", [MyDB_GroupAtt" + to_string(i) + "])";
        groupMatchExpr = i == 0 ? clause : "&& (" + clause + ", " + groupMatchExpr + ")";
        i++;
    }
    groupMatchFunction = combinedRecord->compileComputation(groupMatchExpr);

    // Aggregate computations
    vector<func> aggregateUpdateComputations;
    vector<func> finalAggregateComputations;
    i = 0;
    for (auto& aggPair : aggsToCompute) {
        if (aggPair.first == MyDB_AggType::sum || aggPair.first == MyDB_AggType::avg) {
            aggregateUpdateComputations.push_back(combinedRecord->compileComputation(
                "+ (" + aggPair.second + ", [MyDB_AggAtt" + to_string(i) + "])"));
        } 
		else if (aggPair.first == MyDB_AggType::cnt) {
            aggregateUpdateComputations.push_back(combinedRecord->compileComputation(
                "+ (int[1], [MyDB_AggAtt" + to_string(i) + "])"));
        }

        if (aggPair.first == MyDB_AggType::avg) {
            finalAggregateComputations.push_back(combinedRecord->compileComputation(
                "/ ([MyDB_AggAtt" + to_string(i++) + "], [MyDB_CntAtt])"));
        } 
		else {
            finalAggregateComputations.push_back(combinedRecord->compileComputation(
                "[MyDB_AggAtt" + to_string(i++) + "]"));
        }
    }
    aggregateUpdateComputations.push_back(combinedRecord->compileComputation("+ (int[1], [MyDB_CntAtt])"));

    // Selection function for filtering input records
    func selectionFunction = inputRecord->compileComputation(selectionPredicate);

    // Input iterator
    MyDB_RecordIteratorPtr inputIterator = input->getIterator(inputRecord);
    MyDB_AttValPtr zeroIntValue = make_shared<MyDB_IntAttVal>();

    // Process input records
    while (inputIterator->hasNext()) {
        inputIterator->getNext();

        // Skip record if it doesn't satisfy the predicate
        if (!selectionFunction()->toBool()) continue;

        // Hash based on grouping attributes
        size_t hashValue = 0;
        for (auto& func : groupingComputations) {
            hashValue ^= func()->hash();
        }

        // Retrieve matching records in the hash bucket
        vector<void*>& hashBucket = aggregateHashTable[hashValue];
        void* aggRecordLocation = nullptr;

        // Search for a matching aggregate record in the hash bucket
        bool matchFound = false;
        for (auto& recordLoc : hashBucket) {
            aggRecord->fromBinary(recordLoc);
            combinedRecord->buildFrom(inputRecord, aggRecord);

            if (groupMatchFunction()->toBool()) {
                aggRecordLocation = recordLoc;
                matchFound = true;
                break;
            }
        }

        // If no match found, initialize new aggregate record
        if (!matchFound) {
            i = 0;
            for (auto& func : groupingComputations) {
                aggRecord->getAtt(i++)->set(func());
            }
            for (int j = 0; j < aggregateUpdateComputations.size(); j++) {
                aggRecord->getAtt(i++)->set(zeroIntValue);
            }
        }

        // Update the aggregate values
        i = 0;
        for (auto& func : aggregateUpdateComputations) {
            aggRecord->getAtt(groupCount + i++)->set(func());
        }

        aggRecord->recordContentHasChanged();

        // Append new aggregate record if not found in hash table
        if (!matchFound) {
            aggRecordLocation = currentAggregatePage.appendAndReturnLocation(aggRecord);
            if (aggRecordLocation == nullptr) {
                MyDB_PageReaderWriter newPage(true, *(input->getBufferMgr()));
                currentAggregatePage = newPage;
                aggregatePages.push_back(currentAggregatePage);
                aggRecordLocation = currentAggregatePage.appendAndReturnLocation(aggRecord);
            }
            aggregateHashTable[hashValue].push_back(aggRecordLocation);
        } 
		else {
            aggRecord->toBinary(aggRecordLocation);
        }
    }

    // Output aggregate records
    MyDB_RecordIteratorAltPtr aggregateIterator = getIteratorAlt(aggregatePages);
    MyDB_RecordPtr outputRecord = output->getEmptyRecord();

    // Write final results to output
    while (aggregateIterator->advance()) {
        aggregateIterator->getCurrent(aggRecord);

        for (i = 0; i < groupCount; i++) {
            outputRecord->getAtt(i)->set(aggRecord->getAtt(i));
        }

        combinedRecord->buildFrom(inputRecord, aggRecord);
        for (auto& func : finalAggregateComputations) {
            outputRecord->getAtt(i++)->set(func());
        }

        outputRecord->recordContentHasChanged();
        output->append(outputRecord);
    }
}

#endif