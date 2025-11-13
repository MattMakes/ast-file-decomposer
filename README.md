# Dead Code Detector

AST-based dead code detection for TypeScript/JavaScript projects.

## Installation

```bash
npm install
```

## Usage

```bash
# Analyze current directory
npx tsx detect.ts .

# Analyze specific project
npx tsx detect.ts /path/to/project

# Output as JSON
npx tsx detect.ts /path/to/project --format json

# Output as HTML
npx tsx detect.ts /path/to/project --format html > report.html
```

## How it Works

The detector uses a 7-phase analysis pipeline:

1. **Phase 1: File Scanning** - Discovers all TS/JS files
2. **Phase 2: Symbol Discovery** - Extracts all definitions using AST
3. **Phase 3: Reference Tracking** - Tracks all imports and usages
4. **Phase 4: Escape Hatch Detection** - Finds dynamic code patterns
5. **Phase 5: Entry Point Detection** - Identifies package.json entries, routes, tests
6. **Phase 6: Reachability Analysis** - Mark-and-sweep from entry points
7. **Phase 7: Classification** - Categorizes into safe-to-delete vs needs-review

## Output Categories

**Category 1 (Definitely Dead)**: Safe to auto-delete with 0% false positive risk
- No references found
- Not exported
- No decorators
- No escape hatches in file

**Category 2 (Needs Review)**: Requires human judgment
- Exported but no imports found
- Has decorators  
- Dynamic code patterns in file
- Framework magic methods

## Status

All core files need to be created. Run the setup script to initialize the project.
