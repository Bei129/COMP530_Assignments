
#ifndef PARSER_TYPES_H
#define PARSER_TYPES_H

#include <iostream>
#include <stdlib.h>
#include "ExprTree.h"
#include "MyDB_Catalog.h"
#include "MyDB_Schema.h"
#include "MyDB_Table.h"
#include <string>
#include <utility>

using namespace std;

/*************************************************/
/** HERE WE DEFINE ALL OF THE STRUCTS THAT ARE **/
/** PASSED AROUND BY THE PARSER                **/
/*************************************************/

// structure that encapsulates a parsed computation that returns a value
struct Value {

private:

        // this points to the expression tree that computes this value
        ExprTreePtr myVal;

public:
        ~Value () {}

        Value (ExprTreePtr useMe) {
                myVal = useMe;
        }

        Value () {
                myVal = nullptr;
        }
	
	friend struct CNF;
	friend struct ValueList;
	friend struct SFWQuery;
	#include "FriendDecls.h"
};

// structure that encapsulates a parsed CNF computation
struct CNF {

private:

        // this points to the expression tree that computes this value
        vector <ExprTreePtr> disjunctions;

public:
        ~CNF () {}

        CNF (struct Value *useMe) {
              	disjunctions.push_back (useMe->myVal); 
        }

        CNF () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};

// structure that encapsulates a parsed list of value computations
struct ValueList {

private:

        // this points to the expression tree that computes this value
        vector <ExprTreePtr> valuesToCompute;

public:
        ~ValueList () {}

        ValueList (struct Value *useMe) {
              	valuesToCompute.push_back (useMe->myVal); 
        }

        ValueList () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};


// structure to encapsulate a create table
struct CreateTable {

private:

	// the name of the table to create
	string tableName;

	// the list of atts to create... the string is the att name
	vector <pair <string, MyDB_AttTypePtr>> attsToCreate;

	// true if we create a B+-Tree
	bool isBPlusTree;

	// the attribute to organize the B+-Tree on
	string sortAtt;

public:
	string addToCatalog (string storageDir, MyDB_CatalogPtr addToMe) {

		// make the schema
		MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema>();
		for (auto a : attsToCreate) {
			mySchema->appendAtt (a);
		}

		// now, make the table
		MyDB_TablePtr myTable;

		// just a regular file
		if (!isBPlusTree) {
			myTable =  make_shared <MyDB_Table> (tableName, 
				storageDir + "/" + tableName + ".bin", mySchema);	

		// creating a B+-Tree
		} else {
			
			// make sure that we have the attribute
			if (mySchema->getAttByName (sortAtt).first == -1) {
				cout << "B+-Tree not created.\n";
				return "nothing";
			}
			myTable =  make_shared <MyDB_Table> (tableName, 
				storageDir + "/" + tableName + ".bin", mySchema, "bplustree", sortAtt);	
		}

		// and add to the catalog
		myTable->putInCatalog (addToMe);

		return tableName;
	}

	CreateTable () {}

	CreateTable (string tableNameIn, vector <pair <string, MyDB_AttTypePtr>> atts) {
		tableName = tableNameIn;
		attsToCreate = atts;
		isBPlusTree = false;
	}

	CreateTable (string tableNameIn, vector <pair <string, MyDB_AttTypePtr>> atts, string sortAttIn) {
		tableName = tableNameIn;
		attsToCreate = atts;
		isBPlusTree = true;
		sortAtt = sortAttIn;
	}
	
	~CreateTable () {}

	#include "FriendDecls.h"
};

// structure that stores a list of attributes
struct AttList {

private:

	// the list of attributes
	vector <pair <string, MyDB_AttTypePtr>> atts;

public:
	AttList (string attName, MyDB_AttTypePtr whichType) {
		atts.push_back (make_pair (attName, whichType));
	}

	~AttList () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};

struct FromList {

private:

	// the list of tables and aliases
	vector <pair <string, string>> aliases;

public:
	FromList (string tableName, string aliasName) {
		aliases.push_back (make_pair (tableName, aliasName));
	}

	FromList () {}

	~FromList () {}
	
	friend struct SFWQuery;
	#include "FriendDecls.h"
};


// structure that stores an entire SFW query
struct SFWQuery {

private:

	// the various parts of the SQL query
	vector <ExprTreePtr> valuesToSelect;
	vector <pair <string, string>> tablesToProcess;
	vector <ExprTreePtr> allDisjunctions;
	vector <ExprTreePtr> groupingClauses;

public:
	SFWQuery () {}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf, struct ValueList *grouping) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
		groupingClauses = grouping->valuesToCompute;
	}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
	}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions.push_back (make_shared <BoolLiteral> (true));
	}
	
	~SFWQuery () {}

	void print () {
		cout << "Selecting the following:\n";
		for (auto a : valuesToSelect) {
			cout << "\t" << a->toString () << "\n";
		}
		cout << "From the following:\n";
		for (auto a : tablesToProcess) {
			cout << "\t" << a.first << " AS " << a.second << "\n";
		}
		cout << "Where the following are true:\n";
		for (auto a : allDisjunctions) {
			cout << "\t" << a->toString () << "\n";
		}
		cout << "Group using:\n";
		for (auto a : groupingClauses) {
			cout << "\t" << a->toString () << "\n";
		}
	}

	MyDB_AttTypePtr getType(const ExprTreePtr& expr, const map<string, MyDB_TablePtr>& allTables) {
		if (expr->getExprType() == "StringLiteral") return make_shared<MyDB_StringAttType>();
		if (expr->getExprType() == "IntLiteral") return make_shared<MyDB_IntAttType>();
		if (expr->getExprType() == "DoubleLiteral") return make_shared<MyDB_DoubleAttType>();
		if (expr->getExprType() == "BoolLiteral") return make_shared<MyDB_BoolAttType>();

		string tableName = expr->getTableName();
		string fullTableName;
		for (const auto& table : tablesToProcess) {
			if (table.second == tableName) {
				fullTableName = table.first;
				break;
			}
		}
		if (fullTableName.empty()) {
			cout << "Error: Table " << tableName << " not found.\n";
			return nullptr;
		}

		auto attrName = expr->getAttributeName();
		auto it = allTables.find(fullTableName);
		if (it == allTables.end()) {
			cout << "Error: Table " << fullTableName << " not found in allTables.\n";
			return nullptr;
		}

		auto schema = it->second->getSchema();
		auto attrType = schema->getAttByName(attrName).second;
		/*if (!attrType) {
			cout << "Error: Attribute " << attrName << " not found in table " << fullTableName << ".\n";
		}
		else {
			cout << "Attribute " << attrName << " in table " << fullTableName << " is of type " << attrType->toString() << ".\n";
		}*/
		return attrType;
	}

	MyDB_AttTypePtr checkTypes(const ExprTreePtr& expr, const map<string, MyDB_TablePtr>& allTables) {
		//cout << expr->toString() << endl;
		auto left = expr->getLeftOperand();
		auto right = expr->getRightOperand();
		auto child = expr->getChild();

		MyDB_AttTypePtr leftType = left ? checkTypes(left, allTables) : nullptr;
		MyDB_AttTypePtr rightType = right ? checkTypes(right, allTables) : nullptr;
		MyDB_AttTypePtr childType = child ? checkTypes(child, allTables) : nullptr;

		if (expr->isOr()) {
			if (!leftType || !rightType) return nullptr;
			return leftType;
		}
		else if (expr->isNotOp()) {
			return childType;
		}
		else if (expr->isMathOp()) {
			bool leftIsNumeric = leftType && (leftType->toString() == "int" || leftType->toString() == "double");
			bool rightIsNumeric = rightType && (rightType->toString() == "int" || rightType->toString() == "double");

			bool leftIsString = leftType && (leftType->toString() == "string");
			bool rightIsString = rightType && (rightType->toString() == "string");

			if (expr->isPlusOp()) {
				if ((leftIsNumeric && rightIsNumeric) || (leftIsString && rightIsString)) {
					return leftType;
				}
				return nullptr;
			}

			if (!leftIsNumeric || !rightIsNumeric) {
				cout << "Type error in MathOp: Both operands must be numeric.\n";
				return nullptr; // return nullptr means wrong
			}
			return leftType;  // return a valid type
		}
		else if (expr->isComp() || expr->isEq() || expr->isNotEq()) {
			if (leftType && rightType) {
				bool leftIsNumeric = (leftType->toString() == "int" || leftType->toString() == "double");
				bool rightIsNumeric = (rightType->toString() == "int" || rightType->toString() == "double");

				bool leftIsString = (leftType->toString() == "string");
				bool rightIsString = (rightType->toString() == "string");

				if ((leftIsNumeric && rightIsNumeric) || (leftIsString && rightIsString)) {
					return leftType;
				}
				else {
					cout << "Type error in Comparison: Operands must be of the same type (both numeric or both string).\n";
					return nullptr;
				}
			}
			else {
				cout << "Type error in Comparison: Operand type missing.\n";
				return nullptr;
			}
		}

		// For attribute or constant, simply return their type
		return getType(expr, allTables);
	}


	bool checkAttributes(const ExprTreePtr& expr, const map<string, MyDB_TablePtr>& allTables) {
		if (expr->isAttribute()) {
			//cout << "expr->getTableName():" << expr->getTableName() << endl;
			auto tableName = expr->getTableName(); // aliasName

			string fullTableName;
			for (const auto& table : tablesToProcess) {
				if (table.second == tableName) {
					fullTableName = table.first;
					break;
				}
			}

			auto attrName = expr->getAttributeName();
			auto it = allTables.find(fullTableName);
			if (it == allTables.end() || !it->second->getSchema()->getAttByName(attrName).second) {
				// cout << "Attribute error: Attribute " << attrName << " not found in table " << tableName << ".\n";
				return false;
			}
		}
		return true;
	}

	bool isAggregateOrGrouped(const ExprTreePtr& expr, const vector<ExprTreePtr>& groupingClauses) {
		if (expr->isAggregateFunction()) return true;
		for (const auto& groupExpr : groupingClauses) {
			if (expr->toString() == groupExpr->toString()) return true;
		}
		//cout << "Grouping error: Non-aggregate expression must be part of GROUP BY clause.\n";
		return false;
	}


	void check(MyDB_CatalogPtr myCatalog) {
		cout << endl;
		map <string, MyDB_TablePtr> allTables = MyDB_Table::getAllTables(myCatalog);
		// Make sure that all of the referenced tables exist in the database. 
		for (auto& tableAlias : tablesToProcess) {
			const string& tableName = tableAlias.first;
			if (allTables.find(tableName) == allTables.end()) {
				cout << "Semantic error: Table " << tableName << " does not exist in the database.\n";
				return;
			}
		}

		// Make sure that all of the referenced attributes exist, and are correctly attached to 
		// the tables that are indicated in the query.
		for (auto& expr : valuesToSelect) {
			if (!checkAttributes(expr, allTables)) {
				cout << "Semantic error: Attribute does not exist in the referenced tables.\n";
				return;
			}
		}

		// Make sure that there are no type mismatches in any expressions. For example, it 
		// is valid to compare integers and floating point numbers, but not integers and text
		// strings.For another example, the only arithmetic operation that is valid on a text
		// string is a "+" (which is a concatenation)... anything else should result in an error.
		for (auto& expr : allDisjunctions) {
			if (!checkTypes(expr, allTables)) {
				cout << "Semantic error: Type mismatch in expression.\n";
				return;
			}
		}

		// Make sure that in the case of an aggregation query, the only selected attributes 
		// (other than the aggregates) must be functions of the grouping attributes.
		if (!groupingClauses.empty()) {
			for (auto& expr : valuesToSelect) {
				if (!isAggregateOrGrouped(expr, groupingClauses)) {
					cout << "Semantic error: Non-aggregate attributes must be grouped.\n";
					return;
				}
			}
		}

		cout << "No semantic errors found.\n";
	}

	#include "FriendDecls.h"
};

// structure that sores an entire SQL statement
struct SQLStatement {

private:

	// in case we are a SFW query
	SFWQuery myQuery;
	bool isQuery;

	// in case we a re a create table
	CreateTable myTableToCreate;
	bool isCreate;

public:
	SQLStatement (struct SFWQuery* useMe) {
		myQuery = *useMe;
		isQuery = true;
		isCreate = false;
	}

	SQLStatement (struct CreateTable *useMe) {
		myTableToCreate = *useMe;
		isQuery = false;
		isCreate = true;
	}

	bool isCreateTable () {
		return isCreate;
	}

	bool isSFWQuery () {
		return isQuery;
	}

	string addToCatalog (string storageDir, MyDB_CatalogPtr addToMe) {
		return myTableToCreate.addToCatalog (storageDir, addToMe);
	}		
	
	void printSFWQuery () {
		myQuery.print ();
	}

	void checkSemantics (MyDB_CatalogPtr myCatalog) {
		myQuery.check(myCatalog);
	}

	#include "FriendDecls.h"
};

#endif
