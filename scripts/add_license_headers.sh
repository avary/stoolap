#!/bin/bash
# This script adds the Apache License 2.0 header to Go files that don't already have it.

# Directory to search for Go files
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_FILES=$(find "$ROOT_DIR" -name "*.go" -type f)

# License header template
LICENSE_HEADER="/* Copyright 2025 Stoolap Contributors

 Licensed under the Apache License, Version 2.0 (the \"License\");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an \"AS IS\" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License. */
"

COUNT=0
TOTAL=0

# Process each Go file
for FILE in $GO_FILES; do
    TOTAL=$((TOTAL+1))
    
    # Check if the file already has a license header
    if grep -q "Licensed under the Apache License, Version 2.0" "$FILE"; then
        echo "✓ License header already present in $FILE"
        continue
    fi
    
    # File doesn't have the header, add it
    echo "+ Adding license header to $FILE"
    
    # Create a temporary file with the license header followed by the original content
    TMP_FILE=$(mktemp)
    printf "%s\n" "$LICENSE_HEADER" > "$TMP_FILE"
    cat "$FILE" >> "$TMP_FILE"
    
    # Replace the original file with the temporary file
    mv "$TMP_FILE" "$FILE"
    
    COUNT=$((COUNT+1))
done

echo ""
echo "License headers added to $COUNT/$TOTAL Go files."
echo ""
echo "Note: This script only adds headers to .go files. For other file types,"
echo "please refer to the documentation in CONTRIBUTING.md and add headers manually"
echo "or extend this script as needed."
