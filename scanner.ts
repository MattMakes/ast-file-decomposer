/**
 * Project Scanner - Phase 1
 * Discovers all TypeScript/JavaScript files in the project
 * Based on Section 7.1 of the design document
 */

import { globSync } from 'glob';
import * as path from 'path';

/**
 * Scans project for TypeScript/JavaScript files
 */
export class ProjectScanner {
  /**
   * Scan project for TypeScript/JavaScript files
   * @param projectRoot - Absolute path to project root directory
   * @returns Array of absolute file paths
   */
  scan(projectRoot: string): string[] {
    const files: string[] = [];

    // Include patterns for TS/JS files
    const includePatterns = [
      '**/*.ts',
      '**/*.tsx',
      '**/*.js',
      '**/*.jsx',
      '**/*.mjs',
      '**/*.cjs',
    ];

    // Exclude patterns (build outputs, dependencies, declaration files)
    const excludePatterns = [
      '**/node_modules/**',
      '**/dist/**',
      '**/build/**',
      '**/.next/**',
      '**/out/**',
      '**/coverage/**',
      '**/*.d.ts', // Declaration files (only type definitions)
    ];

    // Scan using glob for each include pattern
    for (const pattern of includePatterns) {
      const matches = globSync(pattern, {
        cwd: projectRoot,
        ignore: excludePatterns,
        absolute: true,
        nodir: true,
      });

      files.push(...matches);
    }

    // Remove duplicates and sort
    const uniqueFiles = Array.from(new Set(files)).sort();

    return uniqueFiles;
  }

  /**
   * Detect language from file extension
   * @param filePath - File path
   * @returns Language identifier
   */
  detectLanguage(filePath: string): 'ts' | 'tsx' | 'js' | 'jsx' {
    const ext = path.extname(filePath);

    switch (ext) {
      case '.ts':
        return 'ts';
      case '.tsx':
        return 'tsx';
      case '.jsx':
        return 'jsx';
      case '.js':
      case '.mjs':
      case '.cjs':
      default:
        return 'js';
    }
  }
}
