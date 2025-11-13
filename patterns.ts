// Minimal patterns file for testing
import type { SgNode } from '@ast-grep/napi';
import type { Location } from './types.ts';

export function nodeHasDecorators(node: SgNode): boolean {
  const decorators = node.findAll({ rule: { kind: 'decorator' } });
  return decorators.length > 0;
}

export function isPrivateMethod(node: SgNode): boolean {
  const text = node.text();
  return text.includes('private ') || text.includes('#');
}

export function isPrivateProperty(node: SgNode): boolean {
  const text = node.text();
  return text.includes('private ') || text.includes('#');
}

export function createSymbolId(filePath: string, name: string, line: number): string {
  return `${filePath}::${name}::${line}`;
}
