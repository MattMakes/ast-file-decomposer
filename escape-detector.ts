import type { EscapeHatchRegistry } from './types.ts';

export class EscapeHatchDetector {
  async detect(files: string[]): Promise<EscapeHatchRegistry> {
    return {
      all: [],
      byFile: new Map(),
      affectedFiles: new Set(),
      affectedSymbols: new Set(),
    };
  }
}
