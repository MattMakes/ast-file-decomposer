# AST File Decomposer

This project contains various utilities that work with JavaScript ASTs. A new `diagram-generator.js` script analyzes dependencies between files and functions and outputs a Mermaid diagram.

## Generating a dependency diagram

Run the following command from the project root:

```bash
npm run diagram
```

This scans all `.js` files in the current directory tree, detects imports and `require()` calls, and finds function calls. A `diagram.mmd` file will be created in the project root with a `graph LR` diagram in Mermaid format.

Functions that have unused parameters are annotated in the diagram.
