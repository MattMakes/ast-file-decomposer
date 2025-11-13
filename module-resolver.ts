/**
 * Module Resolution for TypeScript/JavaScript imports
 * Resolves import paths to absolute file paths
 */

import * as path from 'path';
import * as fs from 'fs';

/**
 * Resolves module import paths to absolute file paths
 * Supports:
 * - Relative imports (./module, ../utils)
 * - TypeScript path aliases (@/*  -> src/*)
 * - Index files (./dir -> ./dir/index.ts)
 * - File extensions (.ts, .tsx, .js, .jsx, .mjs, .cjs)
 */
export class ModuleResolver {
  private projectRoot: string;
  private tsConfig: any;

  constructor(projectRoot: string) {
    this.projectRoot = projectRoot;
    this.tsConfig = this.loadTsConfig();
  }

  /**
   * Resolve an import path to an absolute file path
   * @param importPath - The import path from source code
   * @param fromFile - The absolute path of the file containing the import
   * @returns Absolute file path or null if external dependency
   */
  resolve(importPath: string, fromFile: string): string | null {
    // Relative imports: ./module, ../utils
    if (importPath.startsWith('.')) {
      return this.resolveRelative(importPath, fromFile);
    }

    // Absolute imports: check tsconfig paths for aliases
    if (this.tsConfig?.compilerOptions?.paths) {
      const resolved = this.resolveAlias(importPath);
      if (resolved) return resolved;
    }

    // Node modules (external dependencies - ignore)
    return null;
  }

  /**
   * Resolve relative import paths
   * @param importPath - Relative path (./module, ../utils)
   * @param fromFile - File containing the import
   * @returns Absolute file path
   */
  private resolveRelative(importPath: string, fromFile: string): string {
    const dir = path.dirname(fromFile);
    const resolved = path.resolve(dir, importPath);

    // Try with extensions
    const extensions = ['.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs'];

    for (const ext of extensions) {
      const withExt = resolved + ext;
      if (fs.existsSync(withExt)) {
        return withExt;
      }
    }

    // Try index files
    for (const ext of extensions) {
      const indexFile = path.join(resolved, `index${ext}`);
      if (fs.existsSync(indexFile)) {
        return indexFile;
      }
    }

    // Return as-is if no extension found (might exist)
    return resolved;
  }

  /**
   * Resolve TypeScript path aliases
   * @param importPath - Import path that might match an alias
   * @returns Absolute file path or null
   */
  private resolveAlias(importPath: string): string | null {
    const paths = this.tsConfig.compilerOptions.paths;

    for (const [alias, targets] of Object.entries(paths)) {
      // Handle wildcards: "@/*" : ["src/*"]
      const aliasPattern = alias.replace('*', '(.*)');
      const regex = new RegExp(`^${aliasPattern}$`);
      const match = importPath.match(regex);

      if (match) {
        const captured = match[1] || '';
        const target = (targets as string[])[0].replace('*', captured);
        const resolved = path.resolve(this.projectRoot, target);

        // Try with extensions
        const extensions = ['.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs'];

        for (const ext of extensions) {
          const withExt = resolved + ext;
          if (fs.existsSync(withExt)) {
            return withExt;
          }
        }

        // Try index files
        for (const ext of extensions) {
          const indexFile = path.join(resolved, `index${ext}`);
          if (fs.existsSync(indexFile)) {
            return indexFile;
          }
        }

        return resolved;
      }
    }

    return null;
  }

  /**
   * Load and parse tsconfig.json
   * @returns Parsed tsconfig or null if not found
   */
  private loadTsConfig(): any {
    const tsConfigPath = path.join(this.projectRoot, 'tsconfig.json');
    if (!fs.existsSync(tsConfigPath)) return null;

    try {
      const content = fs.readFileSync(tsConfigPath, 'utf-8');
      return JSON.parse(content);
    } catch {
      return null;
    }
  }
}
