import { readFile } from 'fs/promises';
import { ts, tsx, js } from '@ast-grep/napi';
import type { SgRoot } from '@ast-grep/napi';
import type { Symbol, SymbolTable, Location, SymbolKind } from './types.ts';

export class DefinitionDiscoverer {
  private table: SymbolTable = {
    symbols: new Map(),
    byFile: new Map(),
    exports: new Map(),
    typeOnlySymbols: new Set(),
  };

  async discover(files: string[]): Promise<SymbolTable> {
    for (const file of files) {
      try {
        const content = await readFile(file, 'utf-8');
        const lang = this.detectLang(file);
        const sg = lang.parse(content);
        await this.processFile(sg, file);
      } catch (e) {}
    }
    return this.table;
  }

  private detectLang(file: string) {
    if (file.endsWith('.tsx')) return tsx;
    if (file.endsWith('.ts')) return ts;
    return js;
  }

  private async processFile(sg: SgRoot, file: string) {
    // Extract functions
    const funcs = sg.root().findAll({ rule: { kind: 'function_declaration' } });
    for (const fn of funcs) {
      const name = fn.field('name')?.text();
      if (!name) continue;
      const range = fn.range();
      const sym: Symbol = {
        id: `${file}::${name}::${range.start.line}`,
        name,
        kind: 'function' as SymbolKind,
        filePath: file,
        location: {
          file,
          line: range.start.line,
          column: range.start.column,
          endLine: range.end.line,
          endColumn: range.end.column,
        },
        isExported: fn.text().includes('export'),
        hasDecorators: false,
        scope: 'local',
      };
      this.addSymbol(sym);
    }
  }

  private addSymbol(sym: Symbol) {
    this.table.symbols.set(sym.id, sym);
    const fileSyms = this.table.byFile.get(sym.filePath) || [];
    fileSyms.push(sym);
    this.table.byFile.set(sym.filePath, fileSyms);
  }
}
