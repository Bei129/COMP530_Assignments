#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <functional>

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

// 创建原始表名到别名的映射
std::map<std::string, std::string> createOriginalToAliasMap(
    const std::map<std::string, MyDB_TablePtr> &tablesWithAliases) {
  std::map<std::string, std::string> originalToAlias;
  for (const auto &pair : tablesWithAliases) {
    std::string originalName = pair.second->getName();
    originalToAlias[originalName] = pair.first;
  }
  return originalToAlias;
}

// 更新表达式中的属性别名
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

// 收集表达式中引用的属性
void collectAttributesFromExpr(ExprTreePtr expr, std::set<std::string> &attributes) {
  if (!expr) return;
  if (expr->isId()) {
    attributes.insert(expr->getId());
  }
  if (expr->getLHS()) collectAttributesFromExpr(expr->getLHS(), attributes);
  if (expr->getRHS()) collectAttributesFromExpr(expr->getRHS(), attributes);
  if (expr->getChild()) collectAttributesFromExpr(expr->getChild(), attributes);
}

pair<LogicalOpPtr, double> SFWQuery::optimizeQueryPlan(
    map<string, MyDB_TablePtr> &allTables) {
  MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();
  vector<ExprTreePtr> allDisjunctions = this->allDisjunctions;
  return optimizeQueryPlan(allTables, totSchema, allDisjunctions);
}

pair<LogicalOpPtr, double> SFWQuery::optimizeQueryPlan(
    map<string, MyDB_TablePtr> &allTables, MyDB_SchemaPtr totSchema,
    vector<ExprTreePtr> &allDisjunctions) {
  // 初始化别名与表的映射
  map<string, MyDB_TablePtr> tablesWithAliases;

  // 用于记录每个中间结果表的schema
  std::map<std::string, MyDB_SchemaPtr> tableSchemas;

  // 为每个输入表加上别名并记录schema
  for (auto &tableAliasPair : tablesToProcess) {
    string tableName = tableAliasPair.first;
    string alias = tableAliasPair.second;

    if (allTables.find(tableName) == allTables.end()) {
      cerr << "Error: Table " << tableName << " not found in allTables." << endl;
      continue;
    }

    MyDB_TablePtr aliasedTable = allTables[tableName]->alias(alias);
    tablesWithAliases[alias] = aliasedTable;

    for (auto &att : aliasedTable->getSchema()->getAtts()) {
      totSchema->appendAtt(att);
    }

    tableSchemas[aliasedTable->getName()] = aliasedTable->getSchema();
  }

  // 创建原始表名到别名的映射
  std::map<std::string, std::string> originalToAlias =
      createOriginalToAliasMap(tablesWithAliases);

  // 为所有谓词添加别名前缀
  for (auto &expr : allDisjunctions) {
    expr = updateAttributesWithAlias(expr, originalToAlias);
  }

  // 收集所有 SELECT 和 GROUP BY 中需要的属性
  std::set<std::string> globalRequiredAttributes;

  // 从 SELECT 子句中收集需要的属性
  for (auto &expr : valuesToSelect) {
    collectAttributesFromExpr(expr, globalRequiredAttributes);
  }

  // 从 GROUP BY 子句中收集需要的属性
  for (auto &expr : groupingClauses) {
    collectAttributesFromExpr(expr, globalRequiredAttributes);
  }

  MemoizationMap memo;

  // 递归优化函数
  std::function<PlanInfo(map<string, MyDB_TablePtr> &, vector<ExprTreePtr> &, std::set<std::string>)>
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

    // 处理只有一张表的情况（无 join）
    if (currentTables.size() == 1) {
      auto tableEntry = *currentTables.begin();
      std::string alias = tableEntry.first;
      MyDB_TablePtr table = tableEntry.second;

      std::vector<ExprTreePtr> predicatesForThisTable;
      std::set<std::string> aliases = {alias};
      MyDB_SchemaPtr outputScheme = std::make_shared<MyDB_Schema>();

      // 临时存储用于 JOIN 的属性
      std::set<std::string> joinAttributes;

      // 处理当前谓词，分离出用于本表的谓词和用于 JOIN 的谓词
      for (auto &expr : currentDisjunctions) {
        if (referencesOnlyTables(expr, aliases)) {
          predicatesForThisTable.push_back(expr);
        } else {
          // 处理连接谓词中引用的当前表的属性
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

      // 更新 requiredAttributes 以包含用于 JOIN 的属性
      std::set<std::string> updatedRequiredAttributes = requiredAttributes;
      updatedRequiredAttributes.insert(joinAttributes.begin(), joinAttributes.end());

      // 确保 SELECT 和 GROUP BY 中的属性加入 outputScheme
      for (const auto &attName : updatedRequiredAttributes) {
        // 检查该属性是否属于当前表
        size_t pos = attName.find('_');
        if (pos == std::string::npos) continue;
        std::string tableAlias = attName.substr(0, pos);
        if (tableAlias != alias) continue;

        // 检查该属性是否已经在 outputScheme 中
        bool alreadyExists = std::any_of(
            outputScheme->getAtts().begin(),
            outputScheme->getAtts().end(),
            [&](const std::pair<std::string, std::shared_ptr<MyDB_AttType>> &attPair) {
              return attPair.first == attName;
            });

        if (!alreadyExists) {
          // 从表的 schema 中找到该属性并添加到 outputScheme
          auto it = std::find_if(
              table->getSchema()->getAtts().begin(),
              table->getSchema()->getAtts().end(),
              [&](const std::pair<std::string, std::shared_ptr<MyDB_AttType>> &attPair) {
                return attPair.first == attName;
              });
          if (it != table->getSchema()->getAtts().end()) {
            outputScheme->appendAtt(*it);
            DEBUG_MSG("Added required attribute " + it->first + " to outputScheme of table " + alias);
          } else {
            cerr << "Error: Required attribute " << attName
                 << " not found in table schema." << endl;
          }
        }
      }

      // 同时添加用于 JOIN 的属性到 outputScheme
      for (const auto &att : joinAttributes) {
        // 检查是否已经在 outputScheme 中
        bool alreadyExists = std::any_of(
            outputScheme->getAtts().begin(),
            outputScheme->getAtts().end(),
            [&](const std::pair<std::string, std::shared_ptr<MyDB_AttType>> &attPair) {
              return attPair.first == att;
            });
        if (!alreadyExists) {
          // 从表的 schema 中找到该属性并添加到 outputScheme
          auto it = std::find_if(
              table->getSchema()->getAtts().begin(),
              table->getSchema()->getAtts().end(),
              [&](const std::pair<std::string, std::shared_ptr<MyDB_AttType>> &attPair) {
                return attPair.first == att;
              });
          if (it != table->getSchema()->getAtts().end()) {
            outputScheme->appendAtt(*it);
            DEBUG_MSG("Added join attribute " + it->first + " to outputScheme of table " + alias);
          } else {
            cerr << "Error: Join attribute " << att
                 << " not found in table schema." << endl;
          }
        }
      }

      // 基于谓词计算选择代价
      MyDB_StatsPtr stats = std::make_shared<MyDB_Stats>(table);
      stats = stats->costSelection(predicatesForThisTable);

      if (!stats) {
        cerr << "Error: costSelection returned nullptr for table "
             << table->getName() << endl;
      } else {
        DEBUG_MSG("Selection statistics for table " + table->getName() +
                  ": tuple count = " + std::to_string(stats->getTupleCount()));
      }

      // 创建临时表（中间结果）
      std::string tempTableName = "tempTable" + std::to_string(tempTableId++);
      std::string fileName = "tempTableLoc" + std::to_string(tempTableId - 1) + ".bin";
      MyDB_TablePtr selectTable = std::make_shared<MyDB_Table>(tempTableName, fileName, outputScheme);

      // 创建 LogicalTableScan 操作
      LogicalOpPtr scanOp = std::make_shared<LogicalTableScan>(table, selectTable, stats, predicatesForThisTable);

      // 记录 schema
      tableSchemas[selectTable->getName()] = outputScheme;

      // 更新最佳计划
      bestPlan.op = scanOp;
      bestPlan.cost = stats->getTupleCount();
      bestPlan.tableName = selectTable->getName();

      // 存储在备忘录中
      memo[tableAliases] = bestPlan;
      return bestPlan;
    }

    // 多表情况（需要 join）
    std::vector<std::string> tableNames;
    for (const auto &entry : currentTables) {
      tableNames.push_back(entry.first);
    }

    // 尝试将当前表集划分为左右两部分进行递归优化
    for (size_t i = 1; i < (1 << tableNames.size()) - 1; ++i) {
      map<string, MyDB_TablePtr> leftTables, rightTables;

      for (size_t j = 0; j < tableNames.size(); ++j) {
        if (i & (1 << j)) {
          leftTables[tableNames[j]] = currentTables[tableNames[j]];
        } else {
          rightTables[tableNames[j]] = currentTables[tableNames[j]];
        }
      }

      // 获取左右别名
      std::set<std::string> leftAliases, rightAliases;
      for (const auto &entry : leftTables) leftAliases.insert(entry.first);
      for (const auto &entry : rightTables) rightAliases.insert(entry.first);

      std::vector<ExprTreePtr> leftPredicates, rightPredicates, joinPredicates,
          remainingPredicates;

      // 收集用于 JOIN 的谓词
      std::set<std::string> joinAttributesLeft, joinAttributesRight;

      for (auto &expr : currentDisjunctions) {
        if (referencesOnlyTables(expr, leftAliases)) {
          leftPredicates.push_back(expr);
        } else if (referencesOnlyTables(expr, rightAliases)) {
          rightPredicates.push_back(expr);
        } else if (isJoinPredicate(expr, leftAliases, rightAliases)) {
          // 在左右子树都保留 join 谓词
          joinPredicates.push_back(expr);
          leftPredicates.push_back(expr);
          rightPredicates.push_back(expr);

          // 收集用于 JOIN 的属性
          collectAttributesFromExpr(expr, joinAttributesLeft);
          collectAttributesFromExpr(expr, joinAttributesRight);
        } else {
          remainingPredicates.push_back(expr);
        }
      }

      // 定义左右子计划的所需属性
      std::set<std::string> requiredAttributesLeft;
      std::set<std::string> requiredAttributesRight;

      // 从全局所需属性中提取属于左表和右表的属性
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

      // 添加用于 JOIN 的属性
      requiredAttributesLeft.insert(joinAttributesLeft.begin(), joinAttributesLeft.end());
      requiredAttributesRight.insert(joinAttributesRight.begin(), joinAttributesRight.end());

      // 递归优化左右子计划，传递所需属性（包括 JOIN 属性）
      PlanInfo leftPlan = recursiveOptimize(leftTables, leftPredicates, requiredAttributesLeft);
      PlanInfo rightPlan = recursiveOptimize(rightTables, rightPredicates, requiredAttributesRight);

      MyDB_SchemaPtr leftSchema = tableSchemas[leftPlan.tableName];
      MyDB_SchemaPtr rightSchema = tableSchemas[rightPlan.tableName];

      // 合并左右 schema，添加所有需要的属性（包括 JOIN 属性）
      MyDB_SchemaPtr joinSchema = std::make_shared<MyDB_Schema>();
      for (const auto &att : leftSchema->getAtts()) {
        if (requiredAttributesLeft.find(att.first) != requiredAttributesLeft.end()) {
          joinSchema->appendAtt(att);
        }
      }
      for (const auto &att : rightSchema->getAtts()) {
        if (requiredAttributesRight.find(att.first) != requiredAttributesRight.end()) {
          joinSchema->appendAtt(att);
        }
      }

      // 计算连接统计信息
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

      // 创建临时表（中间结果）
      std::string tempTableNameJoin = "tempTable" + std::to_string(tempTableId++);
      std::string fileNameJoin =
          "tempTableLoc" + std::to_string(tempTableId - 1) + ".bin";
      MyDB_TablePtr joinTable =
          std::make_shared<MyDB_Table>(tempTableNameJoin, fileNameJoin, joinSchema);

      // 创建 LogicalJoin 操作
      LogicalOpPtr joinOp = std::make_shared<LogicalJoin>(
          leftPlan.op, rightPlan.op, joinTable, joinPredicates, joinStats);

      // 如果总成本更低，则更新最佳计划
      if (totalCost < bestPlan.cost) {
        bestPlan.op = joinOp;
        bestPlan.cost = totalCost;
        bestPlan.tableName = joinTable->getName();
        DEBUG_MSG("Updated best plan with join table " + joinTable->getName() +
                  " with cost " + std::to_string(totalCost));
      }

      // 记录 schema
      tableSchemas[joinTable->getName()] = joinSchema;
    }

    // 存储最佳计划到备忘录
    memo[tableAliases] = bestPlan;
    return bestPlan;
  };

  // 初始调用，传递全局需要的属性
  PlanInfo finalPlan = recursiveOptimize(tablesWithAliases, allDisjunctions, globalRequiredAttributes);
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
