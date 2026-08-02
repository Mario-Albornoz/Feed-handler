# Go Syntax Quick Reference

## Package & Imports
```go
package main                    // Executable package
package stats                   // Library package (matches folder name)

import "fmt"                    // Single import
import (                        // Multiple imports
    "fmt"
    "math"
    "github.com/foo/bar"
)
```

## Variables & Types
```go
// Declaration patterns
var count int                   // Declare with zero value (0 for int)
var count int = 10             // Declare with explicit value
var count = 10                 // Type inferred from value
count := 10                    // Short declaration (inside functions only)

// Multiple declarations
var x, y int = 1, 2
a, b := "hello", 3.14

// Basic types
int, int64, uint64             // Integers (int is platform-specific)
float64                        // 64-bit float
string                         // UTF-8 string (immutable)
bool                           // true or false
```

## Structs (like classes without methods)
```go
// Definition
type RollingStats struct {
    Count  int       // Exported (capitalized = public)
    mean   float64   // Unexported (lowercase = private)
}

// Creation
s := RollingStats{}                    // Zero values: Count=0, mean=0.0
s := RollingStats{Count: 5, mean: 10.0}  // Named fields
s := &RollingStats{Count: 5}           // Returns pointer (*RollingStats)

// Access
s.Count = 10
fmt.Println(s.mean)
```

## Pointers
```go
var p *int                     // Pointer to int
x := 42
p = &x                         // & = address-of
fmt.Println(*p)                // * = dereference (42)

// With structs (automatic dereferencing)
s := &RollingStats{}
s.Count = 10                   // No need for (*s).Count
```

## Methods (functions attached to types)
```go
// Value receiver (gets a copy)
func (s RollingStats) IsEmpty() bool {
    return s.Count == 0
}

// Pointer receiver (can modify original)
func (s *RollingStats) Update(value float64) {
    s.Count++                  // Modifies the original
}

// Call both the same way
s := RollingStats{}
s.Update(5.5)
isEmpty := s.IsEmpty()
```

## Functions
```go
// Basic
func add(x int, y int) int {
    return x + y
}

// Multiple return values (very common)
func divide(a, b float64) (float64, error) {
    if b == 0 {
        return 0, fmt.Errorf("division by zero")
    }
    return a / b, nil
}

// Usage
result, err := divide(10, 2)
if err != nil {
    // Handle error
}
```

## Error Handling (no exceptions!)
```go
// Errors are values
func doSomething() error {
    return nil                 // nil = no error
}

// Check immediately after calling
result, err := doSomething()
if err != nil {
    return err                 // Propagate or handle
}
// Continue with result...

// Create errors
import "errors"
err := errors.New("something failed")
err := fmt.Errorf("value %d out of range", x)
```

## Control Flow
```go
// If (no parentheses!)
if x > 10 {
    // ...
}

// If with initialization
if err := doSomething(); err != nil {
    return err
}

// For (only loop type)
for i := 0; i < 10; i++ {      // Traditional
    // ...
}

for i < 10 {                   // While-style
    i++
}

for {                          // Infinite loop
    // ...
}

// Range (iterate slices, maps)
nums := []int{1, 2, 3}
for index, value := range nums {
    fmt.Println(index, value)
}

for _, value := range nums {   // Ignore index with _
    fmt.Println(value)
}
```

## Slices (dynamic arrays)
```go
var s []int                    // nil slice
s = make([]int, 5)            // [0 0 0 0 0] (length 5)
s = []int{1, 2, 3}            // Literal
s = append(s, 4)              // Add element (returns new slice)

// Access
s[0] = 10
length := len(s)
```

## Maps (hash tables)
```go
var m map[string]int          // nil map (can't assign to it!)
m = make(map[string]int)      // Initialize
m = map[string]int{           // Literal
    "apple": 5,
    "orange": 3,
}

// Access
m["key"] = 42
value := m["key"]

// Check existence
value, exists := m["key"]
if exists {
    // Key was found
}

delete(m, "key")              // Remove entry
```

## Interfaces (implicit implementation)
```go
// Definition
type Updater interface {
    Update(value float64)
    Count() int
}

// Any type with these methods satisfies Updater
// No "implements" keyword needed!

type RollingStats struct { /* ... */ }
func (s *RollingStats) Update(value float64) { /* ... */ }
func (s *RollingStats) Count() int { return s.count }

// Now *RollingStats automatically satisfies Updater
```

## Goroutines & Channels
```go
// Goroutine (lightweight thread)
go doWork()                    // Run concurrently
go func() {                    // Anonymous function
    fmt.Println("concurrent")
}()

// Channels (typed queues for goroutines)
ch := make(chan int)          // Unbuffered
ch := make(chan int, 10)      // Buffered (capacity 10)

ch <- 42                      // Send (blocks if full)
value := <-ch                 // Receive (blocks if empty)
close(ch)                     // Close channel

// Select (like switch for channels)
select {
case msg := <-ch1:
    // Received from ch1
case ch2 <- 42:
    // Sent to ch2
case <-time.After(1 * time.Second):
    // Timeout
}
```

## Testing
```go
// In file ending with _test.go
package stats

import "testing"

func TestSomething(t *testing.T) {
    result := Calculate(5)
    if result != 25 {
        t.Errorf("Expected 25, got %d", result)
    }
}
```

## Common Patterns

### Zero Values (defaults)
- `int`, `float64`: `0`
- `string`: `""`
- `bool`: `false`
- `pointer`, `slice`, `map`, `interface`: `nil`

### Naming Conventions
- `UpperCase`: Exported (public)
- `lowerCase`: Unexported (private)
- Interfaces: often end in `-er` (Reader, Writer, Updater)

### The Blank Identifier
```go
_, err := doSomething()       // Ignore first return value
for range items { /* ... */ } // Ignore both index and value
```

### Defer (cleanup)
```go
file, err := os.Open("file.txt")
if err != nil {
    return err
}
defer file.Close()            // Runs when function exits
// ... use file ...
```

## Key Differences from Other Languages

1. **No classes** — structs + methods instead
2. **Explicit error handling** — no exceptions
3. **Multiple return values** — (result, error) pattern everywhere
4. **Implicit interfaces** — no "implements" keyword
5. **Goroutines > threads** — much lighter weight
6. **Channels > shared memory** — preferred concurrency primitive
7. **Pointers but no pointer arithmetic** — safer than C
8. **Capital = public** — naming controls visibility
