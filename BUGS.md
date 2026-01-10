# Bug Fixes

This document tracks bugs that were discovered and fixed in RamSQL.

## Bug #1: Date Comparison with String Literals

### Status: Fixed

### Tests
- `TestCompareDateGT` (`driver/driver_test.go:360`)
- `TestCompareDateLT` (`driver/driver_test.go:416`)

### Symptom
DATE column values could not be compared with string literals in WHERE clauses:
```sql
SELECT dat FROM comp WHERE dat > '2018-03-03'
-- Error: 2018-01-01 00:00:00 +0000 UTC (time.Time) and 2018-03-03 (string) not comparable
```

### Root Cause
When building predicates in `getPredicates()`, the right-side constant was converted based on its token type (string), not the left column's actual type (date). This resulted in `time.Time > string` comparisons that failed.

### Fix
Modified `engine/executor/tx.go` to:
1. Capture the attribute from `RelationAttribute()` to get the left column's type
2. Added `TypeName()` getter to `engine/agnostic/attribute.go`
3. When the right side is a `StringToken` and the left column is a date/timestamp type, use the column's type for conversion

---

## Bug #2: Boolean Literals Parsed as Strings

### Status: Fixed

### Tests
- `TestInsertSingle` (`driver/driver_test.go:1478`)

### Symptom
Boolean literals `true` and `false` in INSERT statements were treated as strings:
```sql
INSERT INTO cat (breed, name, funny) VALUES ('indeterminate', 'Uhura', false)
-- Error: cannot assign 'false' (type string) to cat.funny (type bool)
```

### Root Cause
1. `TrueToken` was completely missing from the lexer
2. `FalseToken` existed but wasn't handled in the value type detection logic
3. Both tokens fell through to default "text" type

### Fix
Modified the following files:
- `engine/parser/lexer.go`: Added `TrueToken` constant and "true" matcher
- `engine/parser/insert.go`: Added `TrueToken` to `parseListElement()`
- `engine/parser/parser.go`: Added `FalseToken` and `TrueToken` to `parseValue()`
- `engine/parser/create.go`: Added `TrueToken` to `parseDefaultClause()`
- `engine/executor/engine.go`: Added boolean type detection in `getValues()` and `getSet()`

---

## Summary

| Bug | Status | Tests Affected |
|-----|--------|----------------|
| Date Comparison | Fixed | TestCompareDateGT, TestCompareDateLT |
| Boolean Literals | Fixed | TestInsertSingle |

All affected tests now pass.
