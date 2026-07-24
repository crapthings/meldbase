# 查询与更新操作符 TODO

> 目标是借鉴 MongoDB 的开发者习惯，但保持 Meldbase 自己的语义、安全边界和跨 Go / TypeScript / 服务端的一致性。
>
> 2026-07-24 状态：现有基线已完成独立审计；`$size`、`$type`、`$all` 和 `$elemMatch` 已贯通 Go、TypeScript、wire、授权、memory/durable、Update/Delete、实时查询和共享 conformance。predicate-work budget 已覆盖执行、Explain、指标和 observer；observer 现可用数组长度与重复率对比 `$all` / `$elemMatch` 的谓词工作放大。

## 设计原则

- [x] 先定义 Meldbase 语义，再决定是否借鉴 MongoDB 的操作符名称。
- [x] 查询协议保持 data-only，不加入脚本、函数、表达式字符串或任意代码。
- [x] 本地、远程、HTTP、WebSocket、实时查询和 Update/Delete 共用同一套语义。
- [x] 明确区分缺失字段、`null`、空数组、空对象和类型不匹配。
- [x] 已开放的操作符都有输入大小、嵌套深度、节点数和执行工作预算；不能安全计费的操作符暂不开放。
- [x] 索引只负责产生候选，最终必须复检完整谓词。
- [x] Explain 说明索引、残余谓词、扫描量和预算压力。
- [x] 不兼容 `$where` 这类任意代码执行能力。

## 当前基线（已支持并完成独立审计）

- [x] 查询：`$eq`、`$ne`、`$gt`、`$gte`、`$lt`、`$lte`、`$in`、`$nin`、`$exists`。
- [x] 逻辑：`$and`、`$or`、`$not`。
- [x] 查询选项：排序、`skip`、`limit`、seek 分页。
- [x] 更新：`$set`、`$unset`、`$inc`、`$push`、`$pull`。
- [x] `_id`、数组标量成员匹配、缺失字段和 `null` 语义。

## 第一阶段：数组查询

### `$elemMatch`

- [x] 已增加可对数组元素遍历逐步计费的 predicate-work budget。
- [x] 支持数组元素为标量时的 `$elemMatch`。
- [x] 支持数组元素为对象时的嵌套字段条件。
- [x] 已定义：缺失、非数组和空数组不匹配；空操作数拒绝。
- [x] 保证多个条件作用于同一个数组元素，不错误拼接不同元素。
- [x] 支持与 `$and`、`$or`、`$not` 嵌套（标量模式和外层查询）。
- [x] 已增加 Go、TypeScript、wire、memory/durable conformance 测试。
- [x] 第一版作为 residual filter；有证据后再做数组索引优化。

### `$all`

- [x] 已复用 predicate-work budget，避免大文档数组绕过查询工作预算。
- [x] 支持数组包含全部指定值。
- [x] 查询值按首次出现顺序去重。
- [x] 空操作数数组拒绝；缺失字段和非数组不匹配。
- [x] 已定义对象值、`null`、缺失字段的结构相等语义；可与 `$elemMatch` 及其他逻辑条件组合。
- [x] 评估多值索引交集：第一版不实现，保持 residual-only。

### `$size`

- [x] 支持数组长度查询。
- [x] 只接受 `0..2^53-1` 的整数；拒绝负数、浮点数和不安全整数。
- [x] 非数组和缺失字段不匹配；空数组匹配 `0`。
- [x] 支持与 `$and`、`$or`、`$not` 和其他字段条件组合。
- [x] 自身作为 residual；可复用同一 `$and` 的其他索引候选，不假装普通 B-tree 索引可用。

## 第二阶段：类型与字符串

### `$type`

- [x] 定义 Meldbase 类型名：`null`、`boolean`、`int64`、`float64`、`string`、`date`、`id`、`binary`、`array`、`object`。
- [x] 不提供 `number` 别名；同时匹配两种数值时显式使用 `["int64", "float64"]`。
- [x] 缺失字段不属于任何类型。
- [x] 支持单个类型和非空类型数组；类型数组表示 OR，并按固定顺序去重 canonicalize。
- [x] 覆盖与 `$exists`、`$not`、`$and`、`$or` 的组合。
- [x] 第一版作为 residual filter，不产生普通 B-tree 索引建议。

### `$regex`

- [ ] 先确认真实需求；前缀搜索优先考虑专用前缀查询。
- [ ] 只匹配字符串字段，非字符串和缺失字段不匹配。
- [ ] 选择安全正则引擎，限制表达式长度、复杂度和执行时间。
- [ ] 明确 UTF-8、大小写、换行、非法正则和 flags 语义。
- [ ] 第一版 Explain 明确标为 residual scan。
- [ ] 禁止把正则文本放入指标 label。

### `$mod`

- [ ] 支持整数取模查询。
- [ ] 除数不能为零，明确负数行为。
- [ ] 默认作为扫描谓词并计入查询预算。
- [ ] 覆盖和范围条件、`$and`、索引字段的组合。

## 第三阶段：字段更新

- [ ] `$min`：新值更小时更新，明确缺失字段行为。
- [ ] `$max`：新值更大时更新，明确缺失字段行为。
- [ ] `$mul`：复用现有数值精度、溢出和非有限值检查。
- [ ] `$rename`：处理父子路径冲突，禁止修改 `_id`。
- [ ] `$currentDate`：先定义 Meldbase Date 类型和时钟来源。
- [ ] 所有更新保持事务原子性、索引、变更事件、实时视图和 replay 一致。
- [ ] 明确 no-op 更新的 `MatchedCount` 和 `ModifiedCount`。

## 第四阶段：数组更新

- [ ] `$addToSet`：只有数组中没有相同值时才追加。
- [ ] `$pop`：删除首项或末项，明确空数组行为。
- [ ] `$pullAll`：删除数组中所有匹配值。
- [ ] `$push` 的 `$each`、`$position`、`$slice`、`$sort`。
- [ ] 固定 `$push` 修饰符执行顺序和 wire canonical order。
- [ ] 对元素数量、值大小和最终文档大小设置限制。

## 第五阶段：Positional Array Update

- [ ] 设计独立 Mutation AST，不把 `$`、`$[]`、`$[name]` 当普通路径字符串。
- [ ] `$`：第一个匹配元素的语义，或设计 Meldbase 更明确的命名。
- [ ] `$[]`：更新所有数组元素。
- [ ] `$[name]`：配合受限 `arrayFilters` 更新匹配元素。
- [ ] 为 `arrayFilters` 增加权限、节点、深度和执行预算。
- [ ] 覆盖嵌套数组、数组对象、空数组、缺失数组和并发更新。
- [ ] 在语义和测试完整前，不开放远程协议。

## 第六阶段：表达能力边界

### `$nor`

- [ ] 评估为 `$not + $or` 的语法糖。
- [ ] 明确空数组和 Explain 展示方式。

### `$expr`

- [ ] 暂不直接照搬 MongoDB `$expr`。
- [ ] 如有真实需求，设计受限、typed、data-only 的表达式 AST。
- [ ] 第一版只考虑字段与常量比较，不开放任意函数。
- [ ] 每个表达式都必须有授权和资源预算。

### `$jsonSchema`

- [x] 暂不直接实现 MongoDB `$jsonSchema`。
- [ ] 先决定 Meldbase schema/validation API 是否独立于查询。
- [x] 查询层优先只实现 `$type` 和字段存在性。

## 明确暂不支持

- [x] `$where`：任意脚本不符合 data-only 和安全审计原则。
- [x] `$text`：由独立全文搜索能力提供。
- [x] `$near`、`$geoWithin`、`$geoIntersects`：需要独立地理类型和空间索引。
- [x] 位运算查询：除非出现明确的权限位图或状态掩码需求。
- [x] 完整 aggregation pipeline：作为独立分析查询项目。

## 协议、SDK 和安全

- [x] 已交付操作符同步 Go AST、TypeScript 类型、wire 编解码和 canonical marshal。
- [x] `$size`、`$type` 已增加共享 Go/TypeScript conformance fixtures。
- [x] 本地和远程共用严格 query wire、匹配语义、顺序和错误分类。
- [x] 服务端按操作符 capability 授权，不只按字段授权。
- [x] 过滤、排序、投影、聚合路径继续独立授权。
- [ ] Explain、诊断、Prometheus、Dashboard 增加低基数操作符分类指标。
- [x] Update/Delete 复用查询规划、完整谓词复检和查询预算，不能绕过限制。
- [ ] 实时 Delta、replay/resume 覆盖新增谓词。

## 验收标准

- [x] `$size`、`$type` 覆盖匹配、非匹配、缺失、`null` 和类型错误。
- [x] `$size`、`$type` 覆盖 `$and`、`$or`、`$not`。
- [ ] 数组操作符覆盖空数组和嵌套数组。
- [x] 当前可索引操作符已有 collection scan 对照测试。
- [x] `$size`、`$type` 的 memory/durable 结果和 Explain 结构一致。
- [ ] 覆盖排序、分页、实时订阅、Update、Delete 和授权拒绝。
- [ ] 覆盖预算、取消、关闭数据库和损坏数据错误。
- [x] observer 提供稳定 JSON 基线，排除耗时和临时路径。
- [x] 通过 `go test ./...`、race、`go vet ./...`、SDK check/test/build 和 `git diff --check`。

## 推进顺序

- [x] 先交付常数级 residual 谓词 `$size`、`$type`。
- [x] 已增加 predicate-work budget 及 Explain/指标计费。
- [ ] 在新预算上交付 `$elemMatch`、`$all`。
- [ ] 根据真实负载决定 `$regex` 或专用前缀查询。
- [ ] `$addToSet`、`$min`、`$max`、`$mul`、`$rename`。
- [ ] `$currentDate`、数组修饰符、positional update。
- [ ] 真实需求出现后再单独立项 `$expr`、全文搜索、地理空间和 aggregation。
- [ ] 每阶段先采集 Explain/observer 数据，再决定是否扩大执行优化。

## 参考

- [ ] [MongoDB Query Predicates](https://www.mongodb.com/docs/manual/reference/mql/query-predicates/)
- [ ] [MongoDB Array Query Predicates](https://www.mongodb.com/docs/manual/reference/mql/query-predicates/arrays/)
- [ ] [MongoDB Logical Query Predicates](https://www.mongodb.com/docs/manual/reference/mql/query-predicates/logical/)
- [ ] [MongoDB Update Operators](https://www.mongodb.com/docs/manual/reference/mql/update/)
