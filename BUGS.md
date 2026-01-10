# Known Bugs and Fix Plan

This document tracks known bugs in RamSQL discovered during testing, with detailed analysis and fix plans.

## Bug #1: Date Comparison with String Literals

### Status: Open

### Failing Tests
- `TestCompareDateGT` (`driver/driver_test.go:360`)
- `TestCompareDateLT` (`driver/driver_test.go:416`)

### Symptom
DATE column values cannot be compared with string literals in WHERE clauses:
```sql
SELECT dat FROM comp WHERE dat > '2018-03-03'
-- Error: 2018-01-01 00:00:00 +0000 UTC (time.Time) and 2018-03-03 (string) not comparable
```

### Root Cause
When building predicates in `getPredicates()` (`engine/executor/tx.go:217`), the right-side constant is converted based on its token type (string), not the left column's actual type (date). This results in `time.Time > string` comparisons that fail.

### Analysis
1. **Type Fetching** (`tx.go:281`): `RelationAttribute()` returns the column's `Attribute` with type info, but it's not captured
2. **Value Conversion** (`tx.go:386-390`): Uses `parser.TypeNameFromToken(rightS.Token)` which returns "text" for string literals
3. **Comparison** (`predicate.go:2244-2285`): `greater()` function cannot compare `time.Time` with `string`

### Files to Modify

| File | Change |
|------|--------|
| `engine/agnostic/attribute.go` | Add `TypeName()` getter method to expose `typeName` field |
| `engine/executor/tx.go:281` | Capture the `Attribute` returned by `RelationAttribute()` |
| `engine/executor/tx.go:386-390` | Use left column's type for converting right-side date literals |

### Fix Strategy
1. Add public `TypeName()` method to `Attribute` struct
2. In `getPredicates()`, capture the left attribute's type info
3. Before converting the right-side constant, check if left is DATE/TIMESTAMP; if so, use that type for conversion
4. The existing `ToInstance()` function already handles date parsing correctly

### Affected Operators
- `>` (greater than)
- `>=` (greater or equal)
- `<` (less than)
- `<=` (less or equal)
- `=` (equal)
- `!=` (not equal)

---

## Bug #2: Boolean Literals Parsed as Strings

### Status: Open

### Failing Tests
- `TestInsertSingle` (`driver/driver_test.go:1478`)

### Symptom
Boolean literals `true` and `false` in INSERT statements are treated as strings:
```sql
INSERT INTO cat (breed, name, funny) VALUES ('indeterminate', 'Uhura', false)
-- Error: cannot assign 'false' (type string) to cat.funny (type bool)
```

### Root Cause
1. `TrueToken` is completely missing from the lexer
2. `FalseToken` exists but isn't handled in the value type detection logic
3. Both tokens fall through to default "text" type

### Analysis

**Lexer** (`engine/parser/lexer.go`):
- Line 83: `FalseToken` defined
- Line 204: Matcher for "false" → FalseToken
- **Missing**: No `TrueToken` definition, no "true" matcher

**Insert Parser** (`engine/parser/insert.go:144`):
- Accepts `FalseToken` in `parseListElement()`
- **Missing**: `TrueToken` not accepted

**Value Type Detection** (`engine/executor/engine.go:328-342`):
- No case for `FalseToken` or `TrueToken`
- Falls to default, treating as "text"

### Files to Modify

| File | Line(s) | Change |
|------|---------|--------|
| `engine/parser/lexer.go` | 83, 204 | Add `TrueToken` constant; add "true" matcher |
| `engine/parser/insert.go` | 144 | Add `TrueToken` to `consumeToken()` call |
| `engine/parser/parser.go` | 674 | Add `FalseToken`, `TrueToken` to `parseValue()` |
| `engine/parser/create.go` | 362 | Add `TrueToken` to `parseDefaultClause()` |
| `engine/executor/engine.go` | 328-342 | Add case for `FalseToken, TrueToken` → "bool" in `getValues()` |
| `engine/executor/engine.go` | 388-402 | Add case for `FalseToken, TrueToken` → "bool" in `getSet()` |

### Fix Strategy
1. Add `TrueToken` constant in lexer
2. Add "true" matcher in lexer
3. Update all consumeToken calls to accept both boolean tokens
4. Add boolean type detection cases in `getValues()` and `getSet()`
5. The existing `ToInstance()` already handles boolean parsing via `strconv.ParseBool()`

---

## Summary

| Bug | Severity | Effort | Impact |
|-----|----------|--------|--------|
| Date Comparison | Medium | Medium | Date/timestamp WHERE clauses with comparison operators |
| Boolean Literals | High | Low | All boolean INSERT/UPDATE operations using literals |

## Test Coverage Note

The main test file `driver/driver_test.go` has 73+ tests covering most functionality. After fixing these bugs, all driver tests should pass. The `bench_test.go` in the root directory requires a sqlite dependency that may have network issues during download.
