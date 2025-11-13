/**
 * Reachability Analysis
 *
 * Implements mark-and-sweep algorithm to determine which symbols
 * are reachable from entry points through the dependency graph.
 *
 * Based on Section 11.1 of the design document.
 */

import type {
  Symbol,
  SymbolTable,
  Reference,
  ReferenceMap,
  EntryPoint,
} from './types.ts';

/**
 * Determine which symbols are reachable from entry points
 */
export class ReachabilityAnalyzer {
  private symbolTable: SymbolTable;
  private referenceMap: ReferenceMap;
  private entryPoints: EntryPoint[];

  constructor(
    symbolTable: SymbolTable,
    referenceMap: ReferenceMap,
    entryPoints: EntryPoint[]
  ) {
    this.symbolTable = symbolTable;
    this.referenceMap = referenceMap;
    this.entryPoints = entryPoints;
  }

  /**
   * Run reachability analysis using mark-and-sweep algorithm
   * Returns a set of reachable symbol IDs
   */
  analyze(): Set<string> {
    const reachable = new Set<string>();
    const queue: string[] = [];

    // PHASE 1: Mark all entry point symbols
    for (const entryPoint of this.entryPoints) {
      const fileSymbols = this.symbolTable.byFile.get(entryPoint.filePath) || [];

      for (const symbol of fileSymbols) {
        // Add all exported symbols from entry points
        if (symbol.isExported) {
          queue.push(symbol.id);
        }

        // Add all top-level statements (side effects)
        if (this.isTopLevelStatement(symbol)) {
          queue.push(symbol.id);
        }
      }
    }

    // PHASE 2: BFS traversal through dependencies
    while (queue.length > 0) {
      const symbolId = queue.shift()!;

      // Skip if already processed
      if (reachable.has(symbolId)) continue;

      // Mark as reachable
      reachable.add(symbolId);

      // Find all symbols this symbol depends on
      const dependencies = this.getDependencies(symbolId);

      for (const depId of dependencies) {
        if (!reachable.has(depId)) {
          queue.push(depId);
        }
      }
    }

    return reachable;
  }

  /**
   * Get all symbols that a given symbol depends on
   *
   * This finds all references that occur within the symbol's scope
   */
  private getDependencies(symbolId: string): string[] {
    const dependencies: string[] = [];
    const symbol = this.symbolTable.symbols.get(symbolId);

    if (!symbol) return dependencies;

    // Get file where symbol is defined
    const filePath = symbol.filePath;

    // Get all references in that file
    const fileRefs = this.referenceMap.byFile.get(filePath) || [];

    for (const ref of fileRefs) {
      // Only consider references that occur within this symbol's scope
      if (this.isWithinSymbolScope(ref, symbol)) {
        dependencies.push(ref.symbolId);
      }
    }

    return dependencies;
  }

  /**
   * Check if a reference occurs within a symbol's scope
   *
   * For simplicity, checks if reference is in same file
   * and within line range of the symbol definition
   */
  private isWithinSymbolScope(ref: Reference, symbol: Symbol): boolean {
    // Reference must be in same file
    if (ref.referencedIn !== symbol.filePath) return false;

    const refLine = ref.location.line;
    const symbolStart = symbol.location.line;
    const symbolEnd = symbol.location.endLine;

    // Reference must be within symbol's line range
    return refLine >= symbolStart && refLine <= symbolEnd;
  }

  /**
   * Check if symbol is a top-level statement (side effect)
   *
   * Top-level statements are not exported but execute on import.
   * Examples: console.log(), global variable assignments, etc.
   *
   * For conservative analysis, we currently return false.
   * TODO: Implement proper detection for side-effect statements
   */
  private isTopLevelStatement(symbol: Symbol): boolean {
    // Conservative: assume no side effects for now
    // This prevents false positives by not marking everything as reachable
    return false;
  }
}
