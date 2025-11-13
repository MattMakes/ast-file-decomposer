# Dead Code Detector - Usage Guide

## Quick Start

```bash
# Analyze current directory
npx tsx detect.ts .

# Analyze a specific project
npx tsx detect.ts /path/to/your/typescript/project

# Get JSON output
npx tsx detect.ts . --format json

# Get HTML report
npx tsx detect.ts . --format html > report.html
```

## Installation

1. Clone or download this repository
2. Install dependencies:
   ```bash
   npm install
   ```

## How It Works

The detector performs a 7-phase analysis:

### Phase 1: File Scanning
Discovers all TypeScript/JavaScript files in the project, excluding:
- `node_modules/`
- `dist/`, `build/`, `out/`
- `.next/`, `coverage/`
- `*.d.ts` declaration files

### Phase 2: Symbol Discovery
Uses AST analysis to extract all code symbols:
- Functions and arrow functions
- Classes and their methods/properties
- Variables and constants
- TypeScript types, interfaces, enums

### Phase 3: Reference Tracking
Tracks all usages of symbols:
- Import statements
- Function calls
- Property access
- Class instantiations

### Phase 4: Escape Hatch Detection
Identifies patterns that prevent static analysis:
- Dynamic imports: `import(path)`
- Reflection: `Object.keys()`, `Reflect.*`
- Eval: `eval()`, `new Function()`
- Decorators: `@Component`
- Computed properties: `obj[variable]`
- String literals containing symbol names

### Phase 5: Entry Point Detection
Finds all project entry points:
- `package.json` main/module/exports/bin
- Next.js pages and routes
- React entry points (src/index.tsx)
- Test files (*.test.*, *.spec.*)
- Config files (vite.config.ts, etc.)

### Phase 6: Reachability Analysis
Uses mark-and-sweep algorithm:
1. Start from all entry points
2. Mark all exported symbols
3. Follow imports and dependencies
4. Mark all reachable symbols

### Phase 7: Classification
Categorizes unreachable symbols:

**Category 1: Definitely Dead** (Safe to auto-delete)
- No references found
- Not exported
- No decorators
- No escape hatches in file
- Not in entry point file
- Not a framework magic method

**Category 2: Needs Review** (Requires human judgment)
- Exported but not imported
- Has decorators
- File contains escape hatches
- Name appears in strings
- Framework special methods (React lifecycle, Next.js exports, etc.)

## Output Formats

### Markdown (default)
Human-readable report with:
- Summary statistics
- List of definitely dead code
- List of code needing review
- Breakdown by symbol type

### JSON
Structured data for programmatic use:
```json
{
  "category1_definitelyDead": [...],
  "category2_needsReview": [...],
  "statistics": {...},
  "metadata": {...}
}
```

### HTML
Interactive web report with:
- Filter buttons (All / Category 1 / Category 2)
- Color-coded categories
- Click-to-view details

## Examples

### Example 1: Analyze a TypeScript project
```bash
npx tsx detect.ts ~/my-app --format markdown > dead-code-report.md
```

### Example 2: Get JSON for CI/CD
```bash
npx tsx detect.ts . --format json > dead-code.json
```

### Example 3: Generate HTML dashboard
```bash
npx tsx detect.ts . --format html > dashboard.html
open dashboard.html
```

## Interpreting Results

### If you see Category 1 items:
✅ These are **safe to delete** with 0% false positive risk.
- The analyzer found no references
- They pass all 8 strict safety rules
- You can safely remove them

### If you see Category 2 items:
⚠️ These **need manual review**:
- Check if exported symbol is actually used externally
- Verify decorators aren't causing framework usage
- Look for dynamic string-based access
- Confirm framework methods aren't called by framework

## Known Limitations

1. **External packages**: Cannot detect if your exports are used by external packages
2. **Dynamic code**: String-based access is detected as escape hatch (conservative)
3. **Test files**: Test files are entry points, so symbols used only in tests are marked as "in use"
4. **Framework magic**: May over-flag framework special methods to prevent false positives

## Tips

- Run regularly during development
- Start with Category 1 (100% safe deletions)
- Review Category 2 carefully
- Use JSON output for CI/CD integration
- Combine with test coverage tools for best results

## Troubleshooting

### "No files found"
- Check you're in the correct directory
- Ensure your project has .ts/.js files
- Check .gitignore isn't excluding source files

### "Module not found" errors
- Run `npm install` to install dependencies
- Ensure you have Node.js 18+ installed

### "Out of memory" errors
- Large projects may need: `NODE_OPTIONS=--max-old-space-size=4096 npx tsx detect.ts .`

## Support

For issues or questions, refer to the source code comments which contain detailed documentation.
