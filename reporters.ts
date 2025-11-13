import type { ClassificationResult } from './types.ts';

export class JSONReporter {
  generate(result: ClassificationResult): string {
    return JSON.stringify(result, null, 2);
  }
}

export class MarkdownReporter {
  generate(result: ClassificationResult): string {
    return `# Dead Code Analysis\n\nTotal symbols: ${result.statistics.totalSymbols}\n`;
  }
}

export class HTMLReporter {
  generate(result: ClassificationResult): string {
    return `<!DOCTYPE html><html><body><h1>Dead Code Report</h1></body></html>`;
  }
}
