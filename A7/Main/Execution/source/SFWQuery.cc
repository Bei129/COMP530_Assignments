#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ParserTypes.h"

#define DEBUG_MSG(str)                                                    \
  do {                                                                    \
    auto now = std::chrono::system_clock::now();                          \
    auto now_c = std::chrono::system_clock::to_time_t(now);               \
    auto now_tm = std::localtime(&now_c);                                 \
    std::cerr << std::put_time(now_tm, "%T") << "  ";                     \
    std::cerr << "DEBUG: " << __FILE__ << "(" << __LINE__ << "): " << str \
              << std::endl;                                               \
    std::cerr.flush();                                                    \
  } while (false)

int SFWQuery::tempTableId = 0;

struct PlanInfo {
  LogicalOpPtr op;
  double cost;
  std::string tableName;
};

// Create a mapping from the original table name to its alias
std::map<std::string, std::string> createOriginalToAliasMap(
    const std::map<std::string, MyDB_TablePtr> &tablesWithAliases) {
  std::map<std::string, std::string> originalToAlias;
  for (const auto &pair : tablesWithAliases) {
    std::string originalName = pair.second->getName();
    originalToAlias[originalName] = pair.first;
  }
  return originalToAlias;
}

// Update attributes in the expression with their corresponding aliases
ExprTreePtr updateAttributesWithAlias(
    ExprTreePtr expr,
    const std::map<std::string, std::string> &originalToAlias) {
  if (!expr) return nullptr;

  if (expr->isId()) {
    std::string id = expr->getId();
    size_t pos = id.find('_');
    if (pos != std::string::npos) {
      std::string tableName = id.substr(0, pos);
      std::string attName = id.substr(pos + 1);

      auto it = originalToAlias.find(tableName);
      if (it != originalToAlias.end()) {
        std::string alias = it->second;
        return make_shared<Identifier>((char *)alias.c_str(),
                                       (char *)attName.c_str());
      }
    }

    return expr;
  } else if (dynamic_cast<EqOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<EqOp>(newLHS, newRHS);
  } else if (dynamic_cast<GtOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<GtOp>(newLHS, newRHS);
  } else if (dynamic_cast<LtOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<LtOp>(newLHS, newRHS);
  } else if (dynamic_cast<NeqOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<NeqOp>(newLHS, newRHS);
  } else if (dynamic_cast<OrOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<OrOp>(newLHS, newRHS);
  } else if (dynamic_cast<PlusOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<PlusOp>(newLHS, newRHS);
  } else if (dynamic_cast<MinusOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<MinusOp>(newLHS, newRHS);
  } else if (dynamic_cast<TimesOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<TimesOp>(newLHS, newRHS);
  } else if (dynamic_cast<DivideOp *>(expr.get()) != nullptr) {
    ExprTreePtr newLHS =
        updateAttributesWithAlias(expr->getLHS(), originalToAlias);
    ExprTreePtr newRHS =
        updateAttributesWithAlias(expr->getRHS(), originalToAlias);
    return make_shared<DivideOp>(newLHS, newRHS);
  } else if (dynamic_cast<NotOp *>(expr.get()) != nullptr) {
    ExprTreePtr newChild =
        updateAttributesWithAlias(expr->getChild(), originalToAlias);
    return make_shared<NotOp>(newChild);
  } else if (dynamic_cast<SumOp *>(expr.get()) != nullptr) {
    ExprTreePtr newChild =
        updateAttributesWithAlias(expr->getChild(), originalToAlias);
    return make_shared<SumOp>(newChild);
  } else if (dynamic_cast<AvgOp *>(expr.get()) != nullptr) {
    ExprTreePtr newChild =
        updateAttributesWithAlias(expr->getChild(), originalToAlias);
    return make_shared<AvgOp>(newChild);
  } else {
    return expr;
  }
}

// Helper function to check if an expression only references a single table
bool referencesOnlyTables(ExprTreePtr expr,
                          const std::set<std::string> &aliases) {
  if (!expr) return true;

  if (expr->isId()) {
    std::string id = expr->getId();
    size_t pos = id.find('_');
    if (pos != std::string::npos) {
      std::string alias = id.substr(0, pos);
      return aliases.count(alias) > 0;
    }
    return false;
  }

  bool res = true;
  if (expr->getLHS()) res &= referencesOnlyTables(expr->getLHS(), aliases);
  if (expr->getRHS()) res &= referencesOnlyTables(expr->getRHS(), aliases);
  if (expr->getChild()) res &= referencesOnlyTables(expr->getChild(), aliases);
  return res;
}

// Check if expression is a join predicate (references tables from both sides)
bool isJoinPredicate(ExprTreePtr expr, const std::set<std::string> &aliasesLeft,
                     const std::set<std::string> &aliasesRight) {
  if (!expr) return false;

  std::set<std::string> referencedAliases;

  // Helper function to collect all aliases in the expression
  std::function<void(ExprTreePtr)> collectAliases = [&](ExprTreePtr e) {
    if (!e) return;
    if (e->isId()) {
      std::string id = e->getId();
      size_t pos = id.find('_');
      if (pos != std::string::npos) {
        std::string alias = id.substr(0, pos);
        referencedAliases.insert(alias);
      }
    }
    if (e->getLHS()) collectAliases(e->getLHS());
    if (e->getRHS()) collectAliases(e->getRHS());
    if (e->getChild()) collectAliases(e->getChild());
  };

  collectAliases(expr);

  bool left = false, right = false;
  for (const auto &alias : referencedAliases) {
    if (aliasesLeft.count(alias)) left = true;
    if (aliasesRight.count(alias)) right = true;
  }

  return left && right;
}

// Define the memoization map type
typedef std::map<std::set<std::string>, PlanInfo> MemoizationMap;

// Collect attributes referenced in an expression
void collectAttributesFromExpr(ExprTreePtr expr,
                               std::set<std::string> &attributes) {
  if (!expr) return;

  if (expr->isId()) {
    attributes.insert(expr->getId());
    DEBUG_MSG("Collected attribute: " + expr->getId());
  } else if (dynamic_cast<SumOp *>(expr.get()) != nullptr ||
             dynamic_cast<AvgOp *>(expr.get()) != nullptr) {
    collectAttributesFromExpr(expr->getChild(), attributes);
  }

  if (expr->getLHS()) collectAttributesFromExpr(expr->getLHS(), attributes);
  if (expr->getRHS()) collectAttributesFromExpr(expr->getRHS(), attributes);
  if (expr->getChild()) collectAttributesFromExpr(expr->getChild(), attributes);
}

// Generate a new schema by selecting only required attributes from an existing schema
MyDB_SchemaPtr projectSchema(const MyDB_SchemaPtr &originalSchema,
                             const std::set<std::string> &requiredAttrs) {
  MyDB_SchemaPtr newSchema = std::make_shared<MyDB_Schema>();
  for (const auto &att : originalSchema->getAtts()) {
    if (requiredAttrs.find(att.first) != requiredAttrs.end()) {
      newSchema->appendAtt(att);
    }
  }
  return newSchema;
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair<LogicalOpPtr, double> SFWQuery::optimizeQueryPlan(
    map<string, MyDB_TablePtr> &allTables) {
  MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();
  vector<ExprTreePtr> allDisjunctions = this->allDisjunctions;
  return optimizeQueryPlan(allTables, totSchema, allDisjunctions);
}


// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair<LogicalOpPtr, double> SFWQuery::optimizeQueryPlan(
    map<string, MyDB_TablePtr> &allTables, MyDB_SchemaPtr totSchema,
    vector<ExprTreePtr> &allDisjunctions) {

  map<string, MyDB_TablePtr> tablesWithAliases;

  std::map<std::string, MyDB_SchemaPtr> tableSchemas;

  for (auto &tableAliasPair : tablesToProcess) {
    string tableName = tableAliasPair.first;
    string alias = tableAliasPair.second;

    if (allTables.find(tableName) == allTables.end()) {
      cerr << "Error: Table " << tableName << " not found in allTables."
           << endl;
      continue;
    }

    MyDB_TablePtr aliasedTable = allTables[tableName]->alias(alias);
    tablesWithAliases[alias] = aliasedTable;

    for (auto &att : aliasedTable->getSchema()->getAtts()) {
      totSchema->appendAtt(att);
    }

    tableSchemas[aliasedTable->getName()] = aliasedTable->getSchema();
  }

  std::map<std::string, std::string> originalToAlias =
      createOriginalToAliasMap(tablesWithAliases);

  for (auto &expr : allDisjunctions) {
    expr = updateAttributesWithAlias(expr, originalToAlias);
  }

  // Collect all attributes required in SELECT and GROUP BY clauses
  std::set<std::string> globalRequiredAttributes;

  // Collect attributes required in the SELECT clause
  for (auto &expr : valuesToSelect) {
    collectAttributesFromExpr(expr, globalRequiredAttributes);
  }

  // // Collect attributes required in the GROUP BY clause
  // for (auto &expr : groupingClauses) {
  //   collectAttributesFromExpr(expr, globalRequiredAttributes);
  // } 

  // // Collect attributes required in the WHERE clause
  //  for (auto &expr : allDisjunctions) {
  //     collectAttributesFromExpr(expr, globalRequiredAttributes);
  // }


  for (const auto &att : globalRequiredAttributes) {
    DEBUG_MSG("Global required attribute: " + att);
  }

  MemoizationMap memo;

  // Recursive optimization function
  std::function<PlanInfo(map<string, MyDB_TablePtr> &, vector<ExprTreePtr> &,
                         std::set<std::string>)>
      recursiveOptimize;

  recursiveOptimize =
      [&](map<string, MyDB_TablePtr> &currentTables,
          vector<ExprTreePtr> &currentDisjunctions,
          std::set<std::string> requiredAttributes) -> PlanInfo {
    std::set<std::string> tableAliases;
    for (const auto &entry : currentTables) {
      tableAliases.insert(entry.first);
    }

    if (memo.count(tableAliases)) {
      return memo[tableAliases];
    }

    PlanInfo bestPlan;
    bestPlan.cost = 9e99;

    // case where no joins
    if (currentTables.size() == 1) {
      auto tableEntry = *currentTables.begin();
      std::string alias = tableEntry.first;
      MyDB_TablePtr table = tableEntry.second;

      std::vector<ExprTreePtr> predicatesForThisTable;
      std::set<std::string> aliases = {alias};

      MyDB_SchemaPtr outputSchema = projectSchema(
            table->getSchema(), requiredAttributes);

      // Temporary storage for attributes used in JOIN (empty in single-table cases)
      std::set<std::string> joinAttributes;

      // Process current predicates, separating those for this table and JOIN predicates
      for (auto &expr : currentDisjunctions) {
        if (referencesOnlyTables(expr, aliases)) {
          predicatesForThisTable.push_back(expr);
        } else {
          // Process attributes from the current table in JOIN predicates
          std::set<std::string> attrsInExpr;
          collectAttributesFromExpr(expr, attrsInExpr);
          for (const auto &att : attrsInExpr) {
            size_t pos = att.find('_');
            if (pos != std::string::npos) {
              std::string tableAlias = att.substr(0, pos);
              if (tableAlias == alias) {
                joinAttributes.insert(att);
              }
            }
          }
        }
      }

      // Update requiredAttributes to include attributes used in JOIN
      std::set<std::string> updated_required_attributes = requiredAttributes;
      updated_required_attributes.insert(joinAttributes.begin(),
                                         joinAttributes.end());

      // Ensure attributes from SELECT and GROUP BY clauses are added to outputSchema
      for (const auto &attName : updated_required_attributes) {
        // Check if the attribute belongs to the current table
        size_t pos = attName.find('_');
        if (pos == std::string::npos) continue;
        std::string tableAlias = attName.substr(0, pos);
        if (tableAlias != alias) continue;

        // Check if the attribute is already in outputSchema
        bool alreadyExists = std::any_of(
            outputSchema->getAtts().begin(), outputSchema->getAtts().end(),
            [&](const std::pair<std::string, std::shared_ptr<MyDB_AttType>>
                    &attPair) { return attPair.first == attName; });

        if (!alreadyExists) {
          // Find the attribute in the table schema and add it to outputSchema
          auto it = std::find_if(
              table->getSchema()->getAtts().begin(),
              table->getSchema()->getAtts().end(),
              [&](const std::pair<std::string, std::shared_ptr<MyDB_AttType>>
                      &attPair) { return attPair.first == attName; });
          if (it != table->getSchema()->getAtts().end()) {
            outputSchema->appendAtt(*it);
            DEBUG_MSG("Added required attribute " + it->first +
                      " to outputSchema of table " + alias);
          } else {
            cerr << "Error: Required attribute " << attName
                 << " not found in table schema." << endl;
          }
        }
      }

      // Also add attributes used in JOIN to outputSchema (usually empty for single-table cases)
      for (const auto &att : joinAttributes) {
        // Check if the attribute is already in outputSchema
        bool alreadyExists = std::any_of(
            outputSchema->getAtts().begin(), outputSchema->getAtts().end(),
            [&](const std::pair<std::string, std::shared_ptr<MyDB_AttType>>
                    &attPair) { return attPair.first == att; });
        if (!alreadyExists) {
          // Find the attribute in the table schema and add it to outputSchema
          auto it = std::find_if(
              table->getSchema()->getAtts().begin(),
              table->getSchema()->getAtts().end(),
              [&](const std::pair<std::string, std::shared_ptr<MyDB_AttType>>
                      &attPair) { return attPair.first == att; });
          if (it != table->getSchema()->getAtts().end()) {
            outputSchema->appendAtt(*it);
            DEBUG_MSG("Added join attribute " + it->first +
                      " to outputSchema of table " + alias);
          } else {
            cerr << "Error: Join attribute " << att
                 << " not found in table schema." << endl;
          }
        }
      }

      MyDB_StatsPtr stats = std::make_shared<MyDB_Stats>(table);
      stats = stats->costSelection(predicatesForThisTable);

      if (!stats) {
        cerr << "Error: costSelection returned nullptr for table "
             << table->getName() << endl;
      } else {
        DEBUG_MSG("Selection statistics for table " + table->getName() +
                  ": tuple count = " + std::to_string(stats->getTupleCount()));
      }

      // Create a temporary table (intermediate result)
      std::string tempTableName = "tempTable" + std::to_string(tempTableId++);
      std::string fileName =
          "tempTableLoc" + std::to_string(tempTableId - 1) + ".bin";
      MyDB_TablePtr selectTable =
          std::make_shared<MyDB_Table>(tempTableName, fileName, outputSchema);

      // Create a LogicalTableScan operation
      LogicalOpPtr scanOp = std::make_shared<LogicalTableScan>(
          table, selectTable, stats, predicatesForThisTable);

      // Record the schema
      tableSchemas[selectTable->getName()] = outputSchema;

      bestPlan.op = scanOp;
      bestPlan.cost = stats->getTupleCount();
      bestPlan.tableName = selectTable->getName();

      memo[tableAliases] = bestPlan;
      return bestPlan;
    }

    // we have at least one join
    std::vector<std::string> tableNames;
    for (const auto &entry : currentTables) {
      tableNames.push_back(entry.first);
    }

    // Divide the current set of tables into left and right subsets for recursive optimization
    for (size_t i = 1; i < (1 << tableNames.size()) - 1; ++i) {
      map<string, MyDB_TablePtr> leftTables, rightTables;

      for (size_t j = 0; j < tableNames.size(); ++j) {
        if (i & (1 << j)) {
          leftTables[tableNames[j]] = currentTables[tableNames[j]];
        } else {
          rightTables[tableNames[j]] = currentTables[tableNames[j]];
        }
      }

      // Get aliases for left and right subsets
      std::set<std::string> leftAliases, rightAliases;
      for (const auto &entry : leftTables) leftAliases.insert(entry.first);
      for (const auto &entry : rightTables) rightAliases.insert(entry.first);

      std::vector<ExprTreePtr> leftPredicates, rightPredicates, joinPredicates,
          remainingPredicates;

      // Collect predicates used in JOIN
      std::set<std::string> joinAttributesLeft, joinAttributesRight;

      for (auto &expr : currentDisjunctions) {
        if (referencesOnlyTables(expr, leftAliases)) {
          leftPredicates.push_back(expr);
        } else if (referencesOnlyTables(expr, rightAliases)) {
          rightPredicates.push_back(expr);
        } else if (isJoinPredicate(expr, leftAliases, rightAliases)) {
          // Keep join predicates in both left and right subtrees
          joinPredicates.push_back(expr);
          leftPredicates.push_back(expr);
          rightPredicates.push_back(expr);

          collectAttributesFromExpr(expr, joinAttributesLeft);
          collectAttributesFromExpr(expr, joinAttributesRight);
        } else {
          remainingPredicates.push_back(expr);
        }
      }

      std::set<std::string> requiredAttributesLeft;
      std::set<std::string> requiredAttributesRight;

      // Extract attributes belonging to left and right subsets from global required attributes
      for (const auto &att : requiredAttributes) {
        size_t pos = att.find('_');
        if (pos != std::string::npos) {
            std::string tableAlias = att.substr(0, pos);
            if (leftAliases.count(tableAlias)) {
                requiredAttributesLeft.insert(att);
            }
            if (rightAliases.count(tableAlias)) {
                requiredAttributesRight.insert(att);
            }
        }
    }

      requiredAttributesLeft.insert(joinAttributesLeft.begin(),
                                    joinAttributesLeft.end());
      requiredAttributesRight.insert(joinAttributesRight.begin(),
                                     joinAttributesRight.end());

      // Recursively optimize left and right subplans, passing required attributes (including JOIN attributes)
      PlanInfo leftPlan =
          recursiveOptimize(leftTables, leftPredicates, requiredAttributesLeft);
      PlanInfo rightPlan = recursiveOptimize(rightTables, rightPredicates,
                                             requiredAttributesRight);

      MyDB_SchemaPtr leftSchema = tableSchemas[leftPlan.tableName];
      MyDB_SchemaPtr rightSchema = tableSchemas[rightPlan.tableName];

      // Use helper function projectSchema to retain only required attributes
      MyDB_SchemaPtr projectedLeftSchema =
          projectSchema(leftSchema, requiredAttributesLeft);
      MyDB_SchemaPtr projectedRightSchema =
          projectSchema(rightSchema, requiredAttributesRight);

      // Merge left and right schemas
      MyDB_SchemaPtr joinSchema = std::make_shared<MyDB_Schema>();
      for (const auto &att : projectedLeftSchema->getAtts()) {
        joinSchema->appendAtt(att);
      }
      for (const auto &att : projectedRightSchema->getAtts()) {
        joinSchema->appendAtt(att);
      }

      // Compute join statistics
      MyDB_StatsPtr joinStats = leftPlan.op->getStats()->costJoin(
          joinPredicates, rightPlan.op->getStats());

      if (!joinStats) {
        cerr << "Error: costJoin returned nullptr." << endl;
        continue;
      }

      DEBUG_MSG("Join statistics between " + leftPlan.tableName + " and " +
                rightPlan.tableName + ": tuple count = " +
                std::to_string(joinStats->getTupleCount()));

      double totalCost =
          leftPlan.cost + rightPlan.cost + joinStats->getTupleCount();

      DEBUG_MSG("Total cost for joining " + leftPlan.tableName + " and " +
                rightPlan.tableName + ": " + std::to_string(totalCost));

      // Create a temporary table for the join (intermediate result)
      std::string tempTableNameJoin =
          "tempTable" + std::to_string(tempTableId++);
      std::string fileNameJoin =
          "tempTableLoc" + std::to_string(tempTableId - 1) + ".bin";
      MyDB_TablePtr joinTable = std::make_shared<MyDB_Table>(
          tempTableNameJoin, fileNameJoin, joinSchema);

      LogicalOpPtr joinOp = std::make_shared<LogicalJoin>(
          leftPlan.op, rightPlan.op, joinTable, joinPredicates, joinStats);

      if (totalCost < bestPlan.cost) {
        bestPlan.op = joinOp;
        bestPlan.cost = totalCost;
        bestPlan.tableName = joinTable->getName();
        DEBUG_MSG("Updated best plan with join table " + joinTable->getName() +
                  " with cost " + std::to_string(totalCost));
      }

      tableSchemas[joinTable->getName()] = joinSchema;
    }

    memo[tableAliases] = bestPlan;
    return bestPlan;
  };

  // Initial call, passing global required attributes
  PlanInfo finalPlan = recursiveOptimize(tablesWithAliases, allDisjunctions,
                                         globalRequiredAttributes);

  // Refine the schema of the final plan
  MyDB_SchemaPtr finalSchema = projectSchema(tableSchemas[finalPlan.tableName], globalRequiredAttributes);

  // Create a new table with the refined schema
  std::string finalTableName = finalPlan.tableName + "_final";
  std::string finalFileName = finalTableName + ".bin";

  MyDB_TablePtr finalTable = std::make_shared<MyDB_Table>(finalTableName, finalFileName, finalSchema);

  // Replace the schema of the table but keep the logical operation tree unchanged
  tableSchemas[finalPlan.tableName] = finalSchema;

  DEBUG_MSG("Final projected schema attributes:");
  for (const auto &att : finalSchema->getAtts()) {
      DEBUG_MSG("  " + att.first);
  }


    return std::make_pair(finalPlan.op, finalPlan.cost);
}

void SFWQuery::print() {
  cout << "Selecting the following:\n";
  for (auto a : valuesToSelect) {
    cout << "\t" << a->toString() << "\n";
  }
  cout << "From the following:\n";
  for (auto a : tablesToProcess) {
    cout << "\t" << a.first << " AS " << a.second << "\n";
  }
  cout << "Where the following are true:\n";
  for (auto a : allDisjunctions) {
    cout << "\t" << a->toString() << "\n";
  }
  cout << "Group using:\n";
  for (auto a : groupingClauses) {
    cout << "\t" << a->toString() << "\n";
  }
}

SFWQuery::SFWQuery(struct ValueList *selectClause, struct FromList *fromClause,
                   struct CNF *cnf, struct ValueList *grouping) {
  valuesToSelect = selectClause->valuesToCompute;
  tablesToProcess = fromClause->aliases;
  allDisjunctions = cnf->disjunctions;
  groupingClauses = grouping->valuesToCompute;
}

SFWQuery::SFWQuery(ValueList *selectClause, FromList *fromClause, CNF *cnf) {
  valuesToSelect = selectClause->valuesToCompute;
  tablesToProcess = fromClause->aliases;
  allDisjunctions = cnf->disjunctions;
}

SFWQuery::SFWQuery(struct ValueList *selectClause,
                   struct FromList *fromClause) {
  valuesToSelect = selectClause->valuesToCompute;
  tablesToProcess = fromClause->aliases;
  allDisjunctions.push_back(make_shared<BoolLiteral>(true));
}

#endif
