
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include <string>
#include <vector>

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
	virtual ~ExprTree () {}

	virtual bool isNumeric() { return false; }
	virtual bool isString() { return false; }
	virtual bool isAggregateFunction() { return false; }
	virtual bool isAttribute() { return false; }
	virtual bool isEq() { return false; }
	virtual bool isOr() { return false; }
	virtual bool isComp() { return false; }
	virtual bool isNotEq() { return false; }
	virtual bool isMathOp() { return false; }
	virtual bool isPlusOp() { return false; }
	virtual bool isNotOp() { return false; }

	virtual string getTableName() { return ""; }
	virtual string getAttributeName() { return ""; }
	virtual ExprTreePtr getLeftOperand() { return nullptr; }
	virtual ExprTreePtr getRightOperand() { return nullptr; }
	virtual ExprTreePtr getChild() { return nullptr; }

	virtual string getExprType() { return "virtual type"; }
};

class BoolLiteral : public ExprTree {

private:
	bool myVal;
public:
	
	BoolLiteral (bool fromMe) {
		myVal = fromMe;
	}

	string toString () {
		if (myVal) {
			return "bool[true]";
		} else {
			return "bool[false]";
		}
	}	

	virtual string getExprType() override { return "BoolLiteral"; }
};

class DoubleLiteral : public ExprTree {

private:
	double myVal;
public:

	DoubleLiteral (double fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "double[" + to_string (myVal) + "]";
	}	

	~DoubleLiteral () {}

	bool isNumeric() override { return true; }

	virtual string getExprType() override { return "DoubleLiteral"; }
};

// this implement class ExprTree
class IntLiteral : public ExprTree {

private:
	int myVal;
public:

	IntLiteral (int fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "int[" + to_string (myVal) + "]";
	}

	~IntLiteral () {}

	bool isNumeric() override { return true; }

	virtual string getExprType() override { return "IntLiteral"; }
};

class StringLiteral : public ExprTree {

private:
	string myVal;
public:

	StringLiteral (char *fromMe) {
		fromMe[strlen (fromMe) - 1] = 0;
		myVal = string (fromMe + 1);
	}

	string toString () {
		return "string[" + myVal + "]";
	}

	~StringLiteral () {}

	bool isString() override { return true; }

	virtual string getExprType() override { return "StringLiteral"; }
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
	}

	string toString () {
		return "[" + tableName + "_" + attName + "]";
	}	

	~Identifier () {}

	bool isAttribute() override { return true; }
	string getTableName() override { return tableName; }
	string getAttributeName() override { return attName; }

	virtual string getExprType() override { return "Identifier"; }
};

class MinusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	MinusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~MinusOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isMathOp() override { return true; }

	virtual string getExprType() override { return "MinusOp"; }
};

class PlusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	PlusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~PlusOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isMathOp() override { return true; }
	bool isPlusOp() override { return true; }

	virtual string getExprType() override { return "PlusOp"; }
};

class TimesOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	TimesOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "* (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~TimesOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isMathOp() override { return true; }

	virtual string getExprType() override { return "TimesOp"; }
};

class DivideOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	DivideOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "/ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~DivideOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isMathOp() override { return true; }

	virtual string getExprType() override { return "DivideOp"; }
};

class GtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	GtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "> (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~GtOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isComp() override { return true; }

	virtual string getExprType() override { return "GtOp"; }
};

class LtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	LtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "< (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~LtOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isComp() override { return true; }

	virtual string getExprType() override { return "LtOp"; }
};

class NeqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	NeqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~NeqOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isNotEq() override { return true; }

	virtual string getExprType() override { return "NeqOp"; }
};

class OrOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	OrOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "|| (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~OrOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isOr() override { return true; }

	virtual string getExprType() override { return "OrOp"; }
};

class EqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	EqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "== (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~EqOp () {}

	ExprTreePtr getLeftOperand() override { return lhs; }
	ExprTreePtr getRightOperand() override { return rhs; }

	bool isEq() override { return true; }

	virtual string getExprType() override { return "EqOp"; }
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "!(" + child->toString () + ")";
	}	

	~NotOp () {}

	ExprTreePtr getChild() override { return child; }

	bool isNotOp() override { return true; }

	virtual string getExprType() override { return "NotOp"; }
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "sum(" + child->toString () + ")";
	}	

	~SumOp () {}

	bool isAggregateFunction() override { return true; }

	virtual string getExprType() override { return "SumOp"; }
};

class AvgOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	AvgOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "avg(" + child->toString () + ")";
	}	

	~AvgOp () {}

	bool isAggregateFunction() override { return true; }

	virtual string getExprType() override { return "AvgOp"; }
};

#endif
