package mvcc

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/semihalev/stoolap/internal/storage"
)

// String handling system
var (
	// Pre-allocated common boolean string values
	scannerBoolStringTrue  = "true"
	scannerBoolStringFalse = "false"

	// String buffer pool for string building operations
	stringBufferPool = sync.Pool{
		New: func() interface{} {
			// 128 bytes is enough for most formatted strings
			buf := make([]byte, 0, 128)
			return &buf
		},
	}

	// Mutex for type-specific caches
	cacheMutex sync.RWMutex

	// Type-specific caches for common values
	// Common integer string cache from -100 to 10000
	scannerIntStringCache map[int64]string

	// Common float string cache (rounded to 2 decimals)
	scannerFloatStringCache map[int64]string

	// Common date/time format strings
	scannerDateFormatCache      map[string]string
	scannerTimeFormatCache      map[string]string
	scannerTimestampFormatCache map[string]string
)

// Initialize caches for string operations
func init() {
	// Initialize other caches
	// Initialize integer string cache for common values
	scannerIntStringCache = make(map[int64]string, 15000)

	// Cache small integers (very common in database IDs and counters)
	for i := int64(-100); i <= 10000; i++ {
		str := strconv.FormatInt(i, 10)
		scannerIntStringCache[i] = str
	}

	// Initialize float string cache for common values
	scannerFloatStringCache = make(map[int64]string, 5000)

	// Initialize date/time format caches
	scannerDateFormatCache = make(map[string]string, 256)
	scannerTimeFormatCache = make(map[string]string, 256)
	scannerTimestampFormatCache = make(map[string]string, 256)
}

// formatDateCached formats a date with caching to reduce allocations
func formatDateCached(t time.Time) string {
	// For common dates (last few days/next few days), use pre-computed values
	// This is a simple but effective optimization for time-series data
	now := time.Now()
	dayDiff := (t.Year()-now.Year())*365 + int(t.Month()-now.Month())*30 + (t.Day() - now.Day())

	// If the date is within a week of today, use a fast path
	if dayDiff >= -7 && dayDiff <= 7 {
		// Create a cache key (year-month-day) without fmt.Sprintf allocation
		y, m, d := t.Year(), t.Month(), t.Day()

		// Get an existing buffer from the pool
		bufPtr := stringBufferPool.Get().(*[]byte)
		buf := *bufPtr
		buf = buf[:0] // Reset but keep capacity

		// Format year with 4 digits (manual formatting to avoid allocations)
		buf = append(buf, byte('0'+y/1000))
		buf = append(buf, byte('0'+(y/100)%10))
		buf = append(buf, byte('0'+(y/10)%10))
		buf = append(buf, byte('0'+y%10))
		buf = append(buf, '-')

		// Format month with 2 digits
		if m < 10 {
			buf = append(buf, '0')
		}
		buf = strconv.AppendInt(buf, int64(m), 10)
		buf = append(buf, '-')

		// Format day with 2 digits
		if d < 10 {
			buf = append(buf, '0')
		}
		buf = strconv.AppendInt(buf, int64(d), 10)

		// Convert buffer to string and intern it
		result := string(buf)

		// Return buffer to pool
		*bufPtr = buf
		stringBufferPool.Put(bufPtr)

		return result
	}

	// For other dates, use the cache
	// Create a direct key without fmt (faster)
	y, m, d := t.Year(), t.Month(), t.Day()

	// Use a simple encoding scheme to avoid string allocation for the key
	// Encode as (year*10000 + month*100 + day) which is unique for all dates
	encodedKey := y*10000 + int(m)*100 + d

	// Check if we have this in a numeric map (faster than string map)
	cacheMutex.RLock()
	if formatted, ok := scannerDateFormatCache[strconv.Itoa(encodedKey)]; ok {
		cacheMutex.RUnlock()
		return formatted
	}
	cacheMutex.RUnlock()

	// Format and cache the result - we use our buffer pool to reduce allocations
	bufPtr := stringBufferPool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0] // Reset but keep capacity

	// Format manually using our buffer
	y, m, d = t.Date()

	// Format year with 4 digits
	buf = append(buf, byte('0'+y/1000))
	buf = append(buf, byte('0'+(y/100)%10))
	buf = append(buf, byte('0'+(y/10)%10))
	buf = append(buf, byte('0'+y%10))
	buf = append(buf, '-')

	// Format month with 2 digits
	if m < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(m), 10)
	buf = append(buf, '-')

	// Format day with 2 digits
	if d < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(d), 10)

	// Convert to string and intern it
	formatted := string(buf)

	// Return buffer to pool
	*bufPtr = buf
	stringBufferPool.Put(bufPtr)

	// Store in cache with a write lock
	cacheMutex.Lock()
	// Limit cache size
	if len(scannerDateFormatCache) < 256 {
		scannerDateFormatCache[strconv.Itoa(encodedKey)] = formatted
	}
	cacheMutex.Unlock()

	return formatted
}

// formatTimeCached formats a time with caching to reduce allocations
func formatTimeCached(t time.Time) string {
	// Use a simple integer key for times (hours*3600 + minutes*60 + seconds)
	// This avoids string allocations for the key
	encodedKey := t.Hour()*3600 + t.Minute()*60 + t.Second()

	// Fast path for common time values (every second, every 5 minutes, etc.)
	// This is very effective for time-series data with regular intervals
	if (t.Second() == 0 && t.Minute()%5 == 0) || // Every 5 minutes on the hour
		(t.Minute() == 0 && t.Second() == 0) { // On the hour

		// Get a buffer from the pool
		bufPtr := stringBufferPool.Get().(*[]byte)
		buf := *bufPtr
		buf = buf[:0] // Reset but keep capacity

		// Format time without allocations
		h, m, s := t.Hour(), t.Minute(), t.Second()

		// Hour with leading zero if needed
		if h < 10 {
			buf = append(buf, '0')
		}
		buf = strconv.AppendInt(buf, int64(h), 10)
		buf = append(buf, ':')

		// Minute with leading zero if needed
		if m < 10 {
			buf = append(buf, '0')
		}
		buf = strconv.AppendInt(buf, int64(m), 10)
		buf = append(buf, ':')

		// Second with leading zero if needed
		if s < 10 {
			buf = append(buf, '0')
		}
		buf = strconv.AppendInt(buf, int64(s), 10)

		// Convert to string and intern it
		formatted := string(buf)

		// Return buffer to pool
		*bufPtr = buf
		stringBufferPool.Put(bufPtr)

		return formatted
	}

	// For other times, check the cache first
	cacheMutex.RLock()
	if formatted, ok := scannerTimeFormatCache[strconv.Itoa(encodedKey)]; ok {
		cacheMutex.RUnlock()
		return formatted
	}
	cacheMutex.RUnlock()

	// Format using buffer pool to avoid allocations
	bufPtr := stringBufferPool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0] // Reset but keep capacity

	// Format time manually
	h, m, s := t.Hour(), t.Minute(), t.Second()

	// Hour with leading zero if needed
	if h < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(h), 10)
	buf = append(buf, ':')

	// Minute with leading zero if needed
	if m < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(m), 10)
	buf = append(buf, ':')

	// Second with leading zero if needed
	if s < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(s), 10)

	// Convert to string and intern it
	formatted := string(buf)

	// Return buffer to pool
	*bufPtr = buf
	stringBufferPool.Put(bufPtr)

	// Store in cache
	cacheMutex.Lock()
	if len(scannerTimeFormatCache) < 256 {
		scannerTimeFormatCache[strconv.Itoa(encodedKey)] = formatted
	}
	cacheMutex.Unlock()

	return formatted
}

// formatTimestampCached formats a timestamp with caching to reduce allocations
func formatTimestampCached(t time.Time) string {
	// Check if it's likely a DATE value (no time component)
	// For DATE values, use the date-only format
	if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
		// Use the date formatter instead
		return formatDateCached(t)
	}

	// For timestamps, we need to cache more aggressively
	// Use a compound numeric key based on all components
	// This avoids string allocations for the key
	encodedKey := (t.Year()*10000+int(t.Month())*100+t.Day())*1000000 +
		t.Hour()*10000 + t.Minute()*100 + t.Second()

	// Check cache first
	cacheKey := strconv.FormatInt(int64(encodedKey), 10)

	cacheMutex.RLock()
	if formatted, ok := scannerTimestampFormatCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		return formatted
	}
	cacheMutex.RUnlock()

	// Format timestamp using buffer pool to avoid allocations
	bufPtr := stringBufferPool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0] // Reset but keep capacity

	// Format date part
	y, m, d := t.Date()
	h, min, s := t.Hour(), t.Minute(), t.Second()

	// Year
	buf = strconv.AppendInt(buf, int64(y), 10)
	buf = append(buf, '-')

	// Month with leading zero
	if m < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(m), 10)
	buf = append(buf, '-')

	// Day with leading zero
	if d < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(d), 10)
	buf = append(buf, 'T') // RFC3339 format

	// Hour with leading zero
	if h < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(h), 10)
	buf = append(buf, ':')

	// Minute with leading zero
	if min < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(min), 10)
	buf = append(buf, ':')

	// Second with leading zero
	if s < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(s), 10)

	// Add timezone
	_, offset := t.Zone()
	if offset == 0 {
		buf = append(buf, 'Z') // UTC
	} else {
		// Format timezone offset
		if offset < 0 {
			buf = append(buf, '-')
			offset = -offset
		} else {
			buf = append(buf, '+')
		}

		// Hours part of offset
		offsetHours := offset / 3600
		if offsetHours < 10 {
			buf = append(buf, '0')
		}
		buf = strconv.AppendInt(buf, int64(offsetHours), 10)
		buf = append(buf, ':')

		// Minutes part of offset
		offsetMinutes := (offset % 3600) / 60
		if offsetMinutes < 10 {
			buf = append(buf, '0')
		}
		buf = strconv.AppendInt(buf, int64(offsetMinutes), 10)
	}

	// Convert to string and intern it
	formatted := string(buf)

	// Return buffer to pool
	*bufPtr = buf
	stringBufferPool.Put(bufPtr)

	// Cache the result
	cacheMutex.Lock()
	if len(scannerTimestampFormatCache) < 256 {
		scannerTimestampFormatCache[cacheKey] = formatted
	}
	cacheMutex.Unlock()

	return formatted
}

// intToStringCached converts an integer to a string with caching
func intToStringCached(i int64) string {
	// Check static cache for common values
	// This is a very fast path for common database values (small integers)
	if i >= -100 && i <= 10000 {
		if s, ok := scannerIntStringCache[i]; ok {
			return s
		}
	}

	// Check if the value is within a reasonable range to cache
	if i >= -1000000 && i <= 1000000 {
		// Use string buffer pool for larger integers
		bufPtr := stringBufferPool.Get().(*[]byte)
		buf := *bufPtr
		buf = buf[:0] // Reset but keep capacity

		// Format directly into buffer without intermediate allocations
		// strconv.AppendInt is allocation-free when appending to an existing buffer
		buf = strconv.AppendInt(buf, i, 10)

		// Intern the string (creates ONE copy that will be reused)
		result := string(buf)

		// Return buffer to pool
		*bufPtr = buf
		stringBufferPool.Put(bufPtr)

		// Cache this value if it's a common range that might be reused
		// This is especially useful for things like primary keys, counters, etc.
		if i >= -10000 && i <= 100000 && i%10 == 0 { // Values divisible by 10 are common
			cacheMutex.Lock()
			if len(scannerIntStringCache) < 15000 { // Allow cache to grow more
				scannerIntStringCache[i] = result
			}
			cacheMutex.Unlock()
		}

		return result
	}

	// Fall back to direct conversion for very large numbers
	// This will allocate, but very large ints are rare in most database workloads
	return strconv.FormatInt(i, 10)
}

// floatToStringCached converts a float to a string with caching
func floatToStringCached(f float64) string {
	// Special case for integers represented as floats (very common)
	if float64(int64(f)) == f {
		return intToStringCached(int64(f))
	}

	// Check if it's a clean value that can be in our cache
	// We cache values like 1.00, 1.25, 1.50, 1.75
	scaledValue := int64(f * 100)
	if f == float64(scaledValue)/100.0 && scaledValue >= -100000 && scaledValue <= 100000 {
		// Check if it's a quarter-value (like X.00, X.25, X.50, X.75)
		if scaledValue%25 == 0 {
			cacheMutex.RLock()
			s, ok := scannerFloatStringCache[scaledValue]
			cacheMutex.RUnlock()
			if ok {
				return s
			}
		}
	}

	// For non-cached values, use buffer pool to avoid allocations
	bufPtr := stringBufferPool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0] // Reset but keep capacity

	// Determine how to format the float
	absFloat := f
	if absFloat < 0 {
		absFloat = -absFloat
	}

	if absFloat < 0.0001 || absFloat >= 1000000 {
		// Scientific notation for very small or very large numbers
		buf = strconv.AppendFloat(buf, f, 'e', -1, 64)
	} else if f == float64(int64(f*100))/100.0 {
		// Format with 2 decimal places for currency-like values
		buf = strconv.AppendFloat(buf, f, 'f', 2, 64)
	} else if f == float64(int64(f*1000))/1000.0 {
		// 3 decimal places for values that are precise to thousandths
		buf = strconv.AppendFloat(buf, f, 'f', 3, 64)
	} else {
		// Dynamic precision for other values
		buf = strconv.AppendFloat(buf, f, 'g', -1, 64)
	}

	// Intern the string (creates ONE copy that will be reused)
	result := string(buf)

	// Return buffer to pool
	*bufPtr = buf
	stringBufferPool.Put(bufPtr)

	// Cache common values with 2 decimal places (typically financial data)
	if f == float64(int64(f*100))/100.0 && f >= -1000 && f <= 1000 {
		key := int64(f * 100)
		cacheMutex.Lock()
		if len(scannerFloatStringCache) < 5000 { // Allow cache to grow more
			scannerFloatStringCache[key] = result
		}
		cacheMutex.Unlock()
	}

	return result
}

// boolToStringCached converts a boolean to a string without allocations
func boolToStringCached(b bool) string {
	// Use pre-allocated static strings for true and false
	// This completely eliminates allocations for boolean values
	if b {
		return scannerBoolStringTrue
	}
	return scannerBoolStringFalse
}

// ScanDirect scans rows directly to typed destinations without interface{} boxing
// This is a specialized version to avoid interface{} allocations in common cases
func scanDirect(value storage.ColumnValue, ptrType reflect.Type, ptrVal reflect.Value) (bool, error) {
	// Only handle specific known types to avoid reflection overhead
	if value == nil {
		return false, nil
	}

	switch ptrType.Elem().Kind() {
	case reflect.Int, reflect.Int64:
		if i, ok := value.AsInt64(); ok {
			ptrVal.Elem().SetInt(i)
			return true, nil
		}
	case reflect.Float64:
		if f, ok := value.AsFloat64(); ok {
			ptrVal.Elem().SetFloat(f)
			return true, nil
		}
	case reflect.String:
		if s, ok := value.AsString(); ok {
			ptrVal.Elem().SetString(s)
			return true, nil
		}
		// Try to convert other types to string
		switch value.Type() {
		case storage.INTEGER:
			if i, ok := value.AsInt64(); ok {
				ptrVal.Elem().SetString(intToStringCached(i))
				return true, nil
			}
		case storage.FLOAT:
			if f, ok := value.AsFloat64(); ok {
				ptrVal.Elem().SetString(floatToStringCached(f))
				return true, nil
			}
		case storage.DATE:
			if d, ok := value.AsDate(); ok {
				ptrVal.Elem().SetString(formatDateCached(d))
				return true, nil
			}
		case storage.TIME:
			if t, ok := value.AsTime(); ok {
				ptrVal.Elem().SetString(formatTimeCached(t))
				return true, nil
			}
		case storage.TIMESTAMP:
			if t, ok := value.AsTimestamp(); ok {
				ptrVal.Elem().SetString(formatTimestampCached(t))
				return true, nil
			}
		case storage.BOOLEAN:
			if b, ok := value.AsBoolean(); ok {
				ptrVal.Elem().SetString(boolToStringCached(b))
				return true, nil
			}
		}
	case reflect.Bool:
		if b, ok := value.AsBoolean(); ok {
			ptrVal.Elem().SetBool(b)
			return true, nil
		}
	case reflect.Struct:
		// Check if it's a time.Time
		if ptrType.Elem() == reflect.TypeOf(time.Time{}) {
			if t, ok := value.AsTimestamp(); ok {
				ptrVal.Elem().Set(reflect.ValueOf(t))
				return true, nil
			}
			if d, ok := value.AsDate(); ok {
				ptrVal.Elem().Set(reflect.ValueOf(d))
				return true, nil
			}
			if t, ok := value.AsTime(); ok {
				ptrVal.Elem().Set(reflect.ValueOf(t))
				return true, nil
			}
		}
	}

	return false, nil
}

// scanNull sets a destination pointer to nil or zero value
func scanNull(dest interface{}) error {
	switch v := dest.(type) {
	case *string:
		*v = ""
	case *int:
		*v = 0
	case *int64:
		*v = 0
	case *float64:
		*v = 0
	case *bool:
		*v = false
	case *time.Time:
		*v = time.Time{}
	case *interface{}:
		*v = nil
	default:
		return fmt.Errorf("unsupported destination type: %T", dest)
	}
	return nil
}

// formatValueAsString formats any value as a string with consistent handling
// This function centralizes all value->string conversions in one place
func formatValueAsString(value storage.ColumnValue) string {
	if value == nil || value.IsNull() {
		return ""
	}

	// Use consistent formatting for each data type
	switch value.Type() {
	case storage.TEXT, storage.JSON:
		if s, ok := value.AsString(); ok {
			return s
		}
	case storage.INTEGER:
		if i, ok := value.AsInt64(); ok {
			return intToStringCached(i)
		}
	case storage.FLOAT:
		if f, ok := value.AsFloat64(); ok {
			return floatToStringCached(f)
		}
	case storage.BOOLEAN:
		if b, ok := value.AsBoolean(); ok {
			return boolToStringCached(b)
		}
	case storage.DATE:
		// DATE should be formatted in YYYY-MM-DD format without time component
		if d, ok := value.AsDate(); ok {
			return formatDateCached(d)
		}
	case storage.TIME:
		if t, ok := value.AsTime(); ok {
			return formatTimeCached(t)
		}
	case storage.TIMESTAMP:
		if t, ok := value.AsTimestamp(); ok {
			return formatTimestampCached(t)
		}
	}

	// Fallback to standard string conversion
	if s, ok := value.AsString(); ok {
		return s
	}

	return ""
}

// scanValue scans a column value into a destination pointer with optimized string handling
// This is a critical hot path in the codebase that needs to be as efficient as possible
func scanValue(value storage.ColumnValue, dest interface{}) error {
	// First attempt to use the valueRef for direct assignments without conversions
	// This is the most efficient path for all types when a DirectValue is used
	if dv, ok := value.(*storage.DirectValue); ok && dv.AsInterface() != nil {
		// Check common destination types for direct assignments
		switch d := dest.(type) {
		case *string:
			if strVal, ok := dv.AsInterface().(string); ok {
				*d = strVal
				return nil
			}
		case *int:
			if intVal, ok := dv.AsInterface().(int); ok {
				*d = intVal
				return nil
			}
			if i64Val, ok := dv.AsInterface().(int64); ok {
				*d = int(i64Val)
				return nil
			}
		case *int64:
			if i64Val, ok := dv.AsInterface().(int64); ok {
				*d = i64Val
				return nil
			}
			if intVal, ok := dv.AsInterface().(int); ok {
				*d = int64(intVal)
				return nil
			}
		case *float64:
			if floatVal, ok := dv.AsInterface().(float64); ok {
				*d = floatVal
				return nil
			}
		case *bool:
			if boolVal, ok := dv.AsInterface().(bool); ok {
				*d = boolVal
				return nil
			}
		case *time.Time:
			if timeVal, ok := dv.AsInterface().(time.Time); ok {
				*d = timeVal
				return nil
			}
		case *interface{}:
			// For interface{} destinations, use the reference directly
			*d = dv.AsInterface()
			return nil
		}
	}

	// Fast path for string destinations (most common case in database results)
	if strPtr, ok := dest.(*string); ok {
		// Use our centralized value formatting function for consistent output
		*strPtr = formatValueAsString(value)
		return nil
	}

	// Handle non-string destinations with optimized code paths
	switch v := dest.(type) {
	case *int:
		// Fast path for integers - common database values
		if i, ok := value.AsInt64(); ok {
			*v = int(i)
			return nil
		}
	case *int64:
		// Direct int64 assignment without conversion
		if i, ok := value.AsInt64(); ok {
			*v = i
			return nil
		}
	case *float64:
		// Direct float64 assignment without conversion
		if f, ok := value.AsFloat64(); ok {
			*v = f
			return nil
		}
		// Also handle integers represented as floats (common in SQL)
		if i, ok := value.AsInt64(); ok {
			*v = float64(i)
			return nil
		}
	case *bool:
		// Direct boolean assignment
		if b, ok := value.AsBoolean(); ok {
			*v = b
			return nil
		}
	case *time.Time:
		// Time types with proper priority order
		// Try timestamp first as it's most common
		if t, ok := value.AsTimestamp(); ok {
			*v = t
			return nil
		}
		// Then try date
		if d, ok := value.AsDate(); ok {
			*v = d
			return nil
		}
		// Finally try time
		if t, ok := value.AsTime(); ok {
			*v = t
			return nil
		}
	case *interface{}:
		// First try to use the DirectValue.AsInterface() method if available
		// This avoids any boxing/unboxing and uses the original value reference
		if dv, ok := value.(*storage.DirectValue); ok {
			if iface := dv.AsInterface(); iface != nil {
				*v = iface
				return nil
			}
		}

		// Fallback to standard conversion methods
		// This avoids unnecessary conversions and string allocations
		switch value.Type() {
		case storage.INTEGER:
			if i, ok := value.AsInt64(); ok {
				*v = i
				return nil
			}
		case storage.FLOAT:
			if f, ok := value.AsFloat64(); ok {
				*v = f
				return nil
			}
		case storage.TEXT:
			if s, ok := value.AsString(); ok {
				// Direct assignment without interning
				*v = s
				return nil
			}
		case storage.BOOLEAN:
			if b, ok := value.AsBoolean(); ok {
				*v = b
				return nil
			}
		case storage.TIMESTAMP:
			if t, ok := value.AsTimestamp(); ok {
				*v = t
				return nil
			}
		case storage.DATE:
			if d, ok := value.AsDate(); ok {
				*v = d
				return nil
			}
		case storage.TIME:
			if t, ok := value.AsTime(); ok {
				*v = t
				return nil
			}
		case storage.JSON:
			if j, ok := value.AsJSON(); ok {
				// Direct assignment without interning
				*v = j
				return nil
			}
		}
	}

	return fmt.Errorf("cannot scan type %T into %T", value, dest)
}

// Int64Slice attaches the methods of sort.Interface to []int64, sorting in increasing order.
// This is used with the standard library sort package.
type Int64Slice []int64

func (x Int64Slice) Len() int           { return len(x) }
func (x Int64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Int64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// SortInt64s sorts a slice of int64s in increasing order.
// This is a backward-compatibility wrapper that uses the SIMD-optimized sort.
func SortInt64s(a []int64) {
	SIMDSortInt64s(a)
}
