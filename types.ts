/**
 * Shared type definitions for dead code detection
 * Based on Section 6 of the design document
 */

/**
 * Type of symbol
 */
export enum SymbolKind {
  Function = 'function',
  ArrowFunction = 'arrow-function',
  Class = 'class',
  ClassMethod = 'class-method',
  ClassProperty = 'class-property',
  Variable = 'variable',
  Constant = 'constant',
  Interface = 'interface',
  TypeAlias = 'type-alias',
  Enum = 'enum',
  EnumMember = 'enum-member',
  Namespace = 'namespace',
}

/**
 * Location in source code
 */
export interface Location {
  file: string;
  line: number;
  column: number;
  endLine: number;
  endColumn: number;
}

/**
 * Represents a code symbol (function, class, variable, etc.)
 */
export interface Symbol {
  /** Unique identifier: "filePath::symbolName::line" */
  id: string;

  /** Symbol name as it appears in code */
  name: string;

  /** Type of symbol */
  kind: SymbolKind;

  /** Absolute path to file containing symbol */
  filePath: string;

  /** Location in source code */
  location: Location;

  /** Whether symbol is exported from its module */
  isExported: boolean;

  /** Whether symbol has decorators */
  hasDecorators: boolean;

  /** Scope visibility */
  scope: 'module' | 'local' | 'global';

  /** Parent symbol (for class members, nested functions) */
  parentId?: string;

  /** TypeScript-specific: is this a type-only symbol? */
  isTypeOnly?: boolean;
}

/**
 * Global registry of all symbols in the project
 */
export interface SymbolTable {
  /** All symbols indexed by ID */
  symbols: Map<string, Symbol>;

  /** Symbols grouped by file */
  byFile: Map<string, Symbol[]>;

  /** Exported symbols indexed by name */
  exports: Map<string, Symbol>;

  /** Type-only symbols (TS interfaces, types) */
  typeOnlySymbols: Set<string>;
}

/**
 * Type of reference to a symbol
 */
export enum ReferenceType {
  Import = 'import',
  DefaultImport = 'default-import',
  Call = 'call',
  Access = 'access',
  Instantiation = 'new',
  TypeReference = 'type-ref',
}

/**
 * Context information for a reference
 */
export interface ReferenceContext {
  /** Is this a type-only reference? */
  isTypeOnly?: boolean;

  /** Import source if this is an import reference */
  importSource?: string;
}

/**
 * Represents a reference to a symbol (import, call, access)
 */
export interface Reference {
  /** ID of the referenced symbol */
  symbolId: string;

  /** File where reference occurs */
  referencedIn: string;

  /** Location of reference */
  location: Location;

  /** Type of reference */
  type: ReferenceType;

  /** Context information */
  context?: ReferenceContext;
}

/**
 * Maps symbols to their references
 */
export interface ReferenceMap {
  /** Symbol ID → array of references to that symbol */
  references: Map<string, Reference[]>;

  /** File → all references in that file */
  byFile: Map<string, Reference[]>;
}

/**
 * Type of escape hatch (pattern that prevents static analysis)
 */
export enum EscapeHatchType {
  DynamicImport = 'dynamic-import',
  Reflection = 'reflection',
  Eval = 'eval',
  Decorator = 'decorator',
  StringLiteral = 'string-literal',
  ComputedProperty = 'computed-property',
}

/**
 * Represents a pattern that prevents static analysis (dynamic code)
 */
export interface EscapeHatch {
  /** File containing the escape hatch */
  filePath: string;

  /** Type of escape hatch */
  type: EscapeHatchType;

  /** Location in code */
  location: Location;

  /** The actual pattern found */
  pattern: string;

  /** Additional context */
  context?: string;
}

/**
 * Registry of all escape hatches in the project
 */
export interface EscapeHatchRegistry {
  /** All escape hatches */
  all: EscapeHatch[];

  /** Escape hatches grouped by file */
  byFile: Map<string, EscapeHatch[]>;

  /** Set of files that have any escape hatches */
  affectedFiles: Set<string>;

  /** Set of symbol IDs in affected files */
  affectedSymbols: Set<string>;
}

/**
 * Type of entry point
 */
export enum EntryPointType {
  PackageMain = 'package-main',
  PackageExports = 'package-exports',
  PackageBin = 'package-bin',
  FrameworkRoute = 'framework-route',
  TestFile = 'test-file',
  ConfigFile = 'config-file',
}

/**
 * Represents an entry point in the project
 */
export interface EntryPoint {
  /** Type of entry point */
  type: EntryPointType;

  /** Absolute path to entry point file */
  filePath: string;

  /** Additional context */
  context?: string;
}

/**
 * Category 1 item (definitely dead)
 */
export interface DeadCodeItem {
  /** The dead symbol */
  symbol: Symbol;

  /** Reason it's classified as dead */
  reason: 'no-references-private-no-escapes';

  /** Always true for Category 1 */
  safeToDelete: true;

  /** Estimated lines of code that can be removed */
  estimatedLOC: number;
}

/**
 * Category 2 item (needs review)
 */
export interface ReviewItem {
  /** The potentially dead symbol */
  symbol: Symbol;

  /** Reasons why it needs human review */
  reasons: string[];

  /** Suspicion level */
  suspicionLevel: 'high' | 'medium' | 'low';

  /** Additional context for reviewer */
  context: string;

  /** Number of references found (if any) */
  referenceCount: number;
}

/**
 * Statistics about the analysis
 */
export interface Statistics {
  /** Total symbols analyzed */
  totalSymbols: number;

  /** Total files analyzed */
  totalFiles: number;

  /** Category 1 count */
  definitelyDeadCount: number;

  /** Category 2 count */
  needsReviewCount: number;

  /** Symbols still in use */
  usedSymbolCount: number;

  /** Potential lines of code saved */
  potentialLOCSaved: number;

  /** Breakdown by symbol kind */
  byKind: Map<SymbolKind, number>;

  /** Breakdown by file */
  byFile: Map<string, number>;
}

/**
 * Metadata about the analysis run
 */
export interface AnalysisMetadata {
  /** Version of the analyzer */
  version: string;

  /** Timestamp of analysis */
  timestamp: string;

  /** Project root analyzed */
  projectRoot: string;

  /** Analysis duration in milliseconds */
  durationMs: number;

  /** Number of entry points found */
  entryPointCount: number;

  /** Number of escape hatches found */
  escapeHatchCount: number;
}

/**
 * Final classification of symbols into categories
 */
export interface ClassificationResult {
  /** Category 1: Safe to auto-delete */
  category1_definitelyDead: DeadCodeItem[];

  /** Category 2: Requires human review */
  category2_needsReview: ReviewItem[];

  /** Overall statistics */
  statistics: Statistics;

  /** Metadata about the analysis */
  metadata: AnalysisMetadata;
}

// ============================================================================
// Builder Classes
// ============================================================================

/**
 * Builder for constructing symbol table
 */
export class SymbolTableBuilder {
  private table: SymbolTable;

  constructor() {
    this.table = {
      symbols: new Map(),
      byFile: new Map(),
      exports: new Map(),
      typeOnlySymbols: new Set(),
    };
  }

  /**
   * Add a symbol to the table
   */
  addSymbol(symbol: Symbol): void {
    this.table.symbols.set(symbol.id, symbol);

    // Index by file
    const fileSymbols = this.table.byFile.get(symbol.filePath) || [];
    fileSymbols.push(symbol);
    this.table.byFile.set(symbol.filePath, fileSymbols);

    // Index exports
    if (symbol.isExported) {
      this.table.exports.set(symbol.name, symbol);
    }

    // Track type-only symbols
    if (symbol.isTypeOnly) {
      this.table.typeOnlySymbols.add(symbol.id);
    }
  }

  /**
   * Build and return the symbol table
   */
  build(): SymbolTable {
    return this.table;
  }
}

/**
 * Tracker for building reference maps
 */
export class ReferenceTracker {
  private map: ReferenceMap;

  constructor() {
    this.map = {
      references: new Map(),
      byFile: new Map(),
    };
  }

  /**
   * Add a reference to the map
   */
  addReference(ref: Reference): void {
    // Index by symbol
    const symbolRefs = this.map.references.get(ref.symbolId) || [];
    symbolRefs.push(ref);
    this.map.references.set(ref.symbolId, symbolRefs);

    // Index by file
    const fileRefs = this.map.byFile.get(ref.referencedIn) || [];
    fileRefs.push(ref);
    this.map.byFile.set(ref.referencedIn, fileRefs);
  }

  /**
   * Get all references to a symbol
   */
  getReferences(symbolId: string): Reference[] {
    return this.map.references.get(symbolId) || [];
  }

  /**
   * Build and return the reference map
   */
  build(): ReferenceMap {
    return this.map;
  }
}
