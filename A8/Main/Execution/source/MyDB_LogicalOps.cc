
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"
#include <algorithm>

string generateSelectionPredicateJoin(vector<ExprTreePtr> exprs)
{
    string result = "";
    if (exprs.size() == 0) {
        return "bool[true]";
    }

    result = exprs[0]->toString();

    for (int i = 1; i < exprs.size(); i++) {
        result = "&& (" + result + ", " + exprs[i]->toString() + ")";
    }

    return result;
}

MyDB_TableReaderWriterPtr LogicalTableScan :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {
	
	// your code here!
	/*cout << "DEBUG: Scan start!" << endl;
	cout << "input schema: " << inputSpec->getSchema() << endl;
	cout << "output schema: " << outputSpec->getSchema() << endl;
	cout << "selection predicate: " << endl;
	for (auto x : selectionPred) {
		cout <<"  "<< x->toString() << endl;
	}*/

    string inputTableName = inputSpec->getName();
    MyDB_BufferManagerPtr bufferMgr = allTableReaderWriters[inputTableName]->getBufferMgr();
    MyDB_TableReaderWriterPtr inputTable = make_shared<MyDB_TableReaderWriter>(inputSpec, bufferMgr);
    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);
    
    vector<string> projections;

    //cout << "DEBUG: projections:" << endl;
    for (auto att : outputSpec->getSchema()->getAtts()) {
        string attName = att.first;
        //size_t pos = attName.find('_');
        //attName = attName.substr(pos + 1);
        projections.push_back("["+ attName +"]");
        //cout << "[" + attName + "]" << endl;
    }

    string selectionPredStr = generateSelectionPredicateJoin(selectionPred);
    //cout << "DEBUG: selectionPredicate: " << endl;
    //cout << "  " << selectionPredStr << endl << endl;

    RegularSelection regularSelection(inputTable, outputTable, selectionPredStr, projections);
    regularSelection.run();

    return outputTable;
}

MyDB_TableReaderWriterPtr LogicalJoin :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// your code here!
    /*cout << "DEBUG: Join start!" << endl;
    cout << "leftInputOp print: " << endl;
    leftInputOp->print();
    cout << "rightInputOp print: " << endl;
    rightInputOp->print();
    cout << "output schema: " << outputSpec->getSchema() << endl;
    cout << "selection predicate: " << endl;
    for (auto x : outputSelectionPredicate) {
        cout << "  " << x->toString() << endl;
    }*/
    MyDB_TableReaderWriterPtr leftTable;
    MyDB_TableReaderWriterPtr rightTable;

    string leftSelectionPredicate = "bool[true]";
    string rightSelectionPredicate = "bool[true]";

    if (leftInputOp->isTableScan()) {
        string leftTableName = leftInputOp->toTableScan()->getInputTableName();
        leftSelectionPredicate = generateSelectionPredicateJoin(leftInputOp->toTableScan()->getOutputSelectionPredicate());
        leftTable = make_shared<MyDB_TableReaderWriter>(leftInputOp->toTableScan()->getInputTable(), allTableReaderWriters[leftTableName]->getBufferMgr());
    }
    else {
        leftTable = leftInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
    }

    if (rightInputOp->isTableScan()) {
        string rightTableName = rightInputOp->toTableScan()->getInputTableName();
        rightSelectionPredicate = generateSelectionPredicateJoin(rightInputOp->toTableScan()->getOutputSelectionPredicate());
        rightTable = make_shared<MyDB_TableReaderWriter>(rightInputOp->toTableScan()->getInputTable(), allTableReaderWriters[rightTableName]->getBufferMgr());
    }
    else {
        rightTable = rightInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
    }

    /*cout << "DEBUG: leftTable schema: " << endl;
    cout << leftTable->getTable()->getSchema() << endl;
    cout << "DEBUG: rightTable schema: " << endl;
    cout << rightTable->getTable()->getSchema() << endl;*/

    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, leftTable->getBufferMgr());

    vector<string> projections;

    //cout << "DEBUG: projections:" << endl;
    for (auto att : outputSpec->getSchema()->getAtts()) {
        string attName = att.first;
        projections.push_back("[" + attName + "]");
        //cout << "[" + attName + "]" << endl;
    }

    string selectionPredStr = generateSelectionPredicateJoin(outputSelectionPredicate);
    vector <pair <string, MyDB_AttTypePtr>> leftAtts = leftTable->getTable()->getSchema()->getAtts();
    vector <pair <string, MyDB_AttTypePtr>> rightAtts = rightTable->getTable()->getSchema()->getAtts();
    cout << "DEBUG: leftAtts:" << endl;
    for (const auto& att : leftAtts) {
        string attName = att.first;
        string attType = att.second->toString();
        cout << "  Attribute Name: " << attName << ", Attribute Type: " << attType << endl;
    }

    cout << "DEBUG: rightAtts:" << endl;
    for (const auto& att : rightAtts) {
        string attName = att.first;
        string attType = att.second->toString();
        cout << "  Attribute Name: " << attName << ", Attribute Type: " << attType << endl;
    }


    //pair <string, string> equalityCheck = make_pair("", "");
    vector<pair<string, string>> equalityChecks;
    cout << "DEBUG: outputSelectionPredicate:" << endl;
    for (auto expr : outputSelectionPredicate) {
        cout << "  " << expr->toString() << endl;
        if (expr->getLHS() && expr->getRHS())
        {
            if (expr->isEq()) {
                string lhs = expr->getLHS()->toString();
                string rhs = expr->getRHS()->toString();
                if (any_of(leftAtts.begin(), leftAtts.end(), [&lhs](const pair<string, MyDB_AttTypePtr>& p)
                    { return lhs == "[" + p.first + "]"; }))
                    equalityChecks.push_back(make_pair(lhs, rhs));
                else
                    equalityChecks.push_back(make_pair(rhs, lhs));
            }
        }
    }
    //cout << "equalityCheck: " << equalityCheck.first <<", " << equalityCheck.second << endl;

    size_t leftTableSize = leftTable->getTable()->lastPage() + 1;
    size_t rightTableSize = rightTable->getTable()->lastPage() + 1;
    size_t bufferNumPages = leftTable->getBufferMgr()->getNumPages();

    if (min(leftTableSize, rightTableSize) > bufferNumPages / 2) {
        SortMergeJoin sortMergeJoin(leftTable, rightTable, outputTable, selectionPredStr, projections, equalityChecks.empty()?make_pair("",""):equalityChecks[0], leftSelectionPredicate, rightSelectionPredicate);
        sortMergeJoin.run();
        cout << "DEBUG: Sort merge join" << endl;
    }
    else {
        ScanJoin scanJoin(leftTable, rightTable, outputTable, selectionPredStr, projections, equalityChecks, leftSelectionPredicate, rightSelectionPredicate);
        scanJoin.run();
        cout << "DEBUG: Scan join" << endl;
    }
    
    MyDB_BufferManagerPtr bufferMgr = leftTable->getBufferMgr();
    bufferMgr->killTable(leftTable->getTable());
    bufferMgr->killTable(rightTable->getTable());

	return outputTable;
}

#endif
