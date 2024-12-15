
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"
#include <algorithm>

string generateSelectionPredicate(vector<ExprTreePtr> exprs)
{
    string result = "";
    if (exprs.size() == 0) {
        return "bool[true]";
    }

    result = exprs[0]->toAttString();

    for (int i = 1; i < exprs.size(); i++) {
        result = "&& (" + result + ", " + exprs[i]->toAttString() + ")";
    }

    return result;
}

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

    string inputTableName = this->getInputTableName();
    MyDB_TableReaderWriterPtr inputTable = allTableReaderWriters[inputTableName];
    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, inputTable->getBufferMgr());
    
    vector<string> projections;

    //cout << "DEBUG: projections:" << endl;
    for (auto att : outputSpec->getSchema()->getAtts()) {
        string attName = att.first;
        size_t pos = attName.find('_');
        attName = attName.substr(pos + 1);
        projections.push_back("["+ attName +"]");
        //cout << "[" + attName + "]" << endl;
    }

    string selectionPredStr = generateSelectionPredicate(selectionPred);
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

    MyDB_TableReaderWriterPtr leftTable = leftInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
    MyDB_TableReaderWriterPtr rightTable = rightInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);

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


    pair <string, string> equalityCheck;
    for (auto expr : outputSelectionPredicate) {
        if (expr->getLHS() && expr->getRHS())
        {
            string lhs = expr->getLHS()->toString();
            string rhs = expr->getRHS()->toString();
            if (any_of(leftAtts.begin(), leftAtts.end(), [&lhs](const pair<string, MyDB_AttTypePtr>& p)
                { return lhs == "[" + p.first + "]"; }))
                equalityCheck = make_pair(lhs, rhs);
            else
                equalityCheck = make_pair(rhs, lhs);
            break;
        }
    }
    cout << "equalityCheck: " << equalityCheck.first <<", " << equalityCheck.second << endl;

    string leftSelectionPredicate = "bool[true]";
    string rightSelectionPredicate = "bool[true]";

    SortMergeJoin sortMergeJoin(leftTable, rightTable, outputTable, selectionPredStr, projections, equalityCheck, leftSelectionPredicate, rightSelectionPredicate);
    sortMergeJoin.run();

    MyDB_BufferManagerPtr bufferMgr = leftTable->getBufferMgr();
    bufferMgr->killTable(leftTable->getTable());
    bufferMgr->killTable(rightTable->getTable());

	return outputTable;
}

#endif
