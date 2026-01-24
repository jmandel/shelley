---
name: json-formatter
description: Format and validate JSON data, pretty-print JSON, and extract values using jq.
license: Apache-2.0
compatibility: Requires jq command-line tool
allowed-tools: bash
metadata:
  author: shelley-examples
  version: "1.0"
  requires: jq
---

# JSON Formatter Skill

This skill provides JSON formatting, validation, and querying capabilities using the `jq` command-line tool.

## Requirements

- `jq` - JSON processor (install with `apt install jq` or `brew install jq`)

## Capabilities

### 1. Pretty-Print JSON

Format JSON with proper indentation:

```bash
echo '{"name":"Alice","age":30,"city":"NYC"}' | jq '.'
```

Output:
```json
{
  "name": "Alice",
  "age": 30,
  "city": "NYC"
}
```

### 2. Validate JSON

Check if a string is valid JSON:

```bash
echo '{"valid": "json"}' | jq '.' > /dev/null && echo "Valid JSON" || echo "Invalid JSON"
```

### 3. Extract Values

Extract specific fields:

```bash
# Extract a single field
echo '{"name":"Alice","age":30}' | jq '.name'
# Output: "Alice"

# Extract nested fields
echo '{"user":{"name":"Alice","age":30}}' | jq '.user.name'
# Output: "Alice"

# Extract array elements
echo '{"items":["a","b","c"]}' | jq '.items[0]'
# Output: "a"
```

### 4. Filter and Transform

Apply filters and transformations:

```bash
# Select objects from an array
echo '[{"name":"Alice","age":30},{"name":"Bob","age":25}]' | jq '.[] | select(.age > 26)'

# Map values
echo '[1,2,3,4,5]' | jq 'map(. * 2)'
# Output: [2,4,6,8,10]

# Get keys
echo '{"a":1,"b":2,"c":3}' | jq 'keys'
# Output: ["a","b","c"]
```

### 5. Format from Files

Read and format JSON files:

```bash
# Pretty-print a file
jq '.' input.json

# Save formatted output
jq '.' input.json > output.json

# Compact format (no whitespace)
jq -c '.' input.json
```

### 6. Common Patterns

#### Combine multiple JSONs into array
```bash
jq -s '.' file1.json file2.json
```

#### Extract specific fields from array
```bash
echo '[{"name":"Alice","age":30},{"name":"Bob","age":25}]' | jq '.[].name'
```

#### Count array elements
```bash
echo '{"items":["a","b","c"]}' | jq '.items | length'
```

#### Sort by field
```bash
echo '[{"name":"Bob","age":25},{"name":"Alice","age":30}]' | jq 'sort_by(.age)'
```

## Error Handling

If `jq` is not installed:

```bash
if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed"
    echo "Install with: apt install jq (Debian/Ubuntu) or brew install jq (macOS)"
    exit 1
fi
```

## Usage Examples

### Example 1: Format API Response

```bash
curl -s https://api.example.com/data | jq '.'
```

### Example 2: Extract Specific Data

```bash
# Extract all email addresses from a JSON file
jq -r '.users[].email' users.json
```

### Example 3: Validate Before Processing

```bash
if echo "$JSON_STRING" | jq '.' > /dev/null 2>&1; then
    echo "Valid JSON - processing..."
    echo "$JSON_STRING" | jq '.data.results'
else
    echo "Invalid JSON - cannot process"
fi
```

## Tips

- Use `-r` flag for raw output (no quotes around strings)
- Use `-c` flag for compact output (single line)
- Use `-s` flag to slurp multiple JSON objects into an array
- Use `jq --help` to see all available options
- Test complex queries at [jqplay.org](https://jqplay.org)

## Common Issues

**Issue**: "jq: command not found"  
**Solution**: Install jq with your package manager

**Issue**: "parse error: Invalid numeric literal"  
**Solution**: Ensure your JSON is properly formatted with quotes around strings

**Issue**: Output includes extra quotes  
**Solution**: Use `-r` flag for raw output: `jq -r '.field'`
