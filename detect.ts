#!/usr/bin/env node

/**
 * Main Entry Point - Dead Code Detector
 * Orchestrates the 7-phase analysis pipeline
 * Based on Section 7 of the design document
 */

import { ProjectScanner } from './scanner.ts';
import { DefinitionDiscoverer } from './symbol-table.ts';
import { ReferenceTracker } from './reference-tracker.ts';
import { EscapeHatchDetector } from './escape-detector.ts';
import { EntryPointDetector } from './entry-detector.ts';
import { ReachabilityAnalyzer } from './reachability.ts';
import { SymbolClassifier } from './classifier.ts';
import { JSONReporter, MarkdownReporter, HTMLReporter } from './reporters.ts';
import * as path from 'path';
import type { ClassificationResult } from './types.ts';

/**
 * Parse command-line arguments
 */
function parseArgs(): { projectPath: string; format: 'json' | 'markdown' | 'html' } {
  const args = process.argv.slice(2);

  if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
    console.error(`
Dead Code Detector - AST-based analysis for TypeScript/JavaScript

Usage:
  node detect.ts <project-path> [--format <format>]

Arguments:
  <project-path>    Path to project directory to analyze

Options:
  --format <format> Output format: json, markdown, or html (default: markdown)
  --help, -h        Show this help message

Examples:
  node detect.ts /path/to/project
  node detect.ts . --format json
  node detect.ts ~/myproject --format html > report.html
    `);
    process.exit(args.includes('--help') || args.includes('-h') ? 0 : 1);
  }

  const projectPath = args[0];
  let format: 'json' | 'markdown' | 'html' = 'markdown';

  // Parse --format flag
  const formatIndex = args.indexOf('--format');
  if (formatIndex !== -1 && args[formatIndex + 1]) {
    const formatArg = args[formatIndex + 1].toLowerCase();
    if (formatArg === 'json' || formatArg === 'markdown' || formatArg === 'html') {
      format = formatArg;
    } else {
      console.error(`Error: Invalid format "${formatArg}". Must be json, markdown, or html.`);
      process.exit(1);
    }
  }

  return { projectPath, format };
}

/**
 * Show progress message
 */
function progress(phase: number, message: string): void {
  console.error(`[Phase ${phase}/7] ${message}`);
}

/**
 * Main analysis function
 */
async function analyze(projectPath: string): Promise<ClassificationResult> {
  const startTime = Date.now();

  // Resolve absolute path
  const projectRoot = path.resolve(projectPath);

  progress(1, 'Scanning project files...');
  const scanner = new ProjectScanner();
  const files = scanner.scan(projectRoot);
  console.error(`Found ${files.length} files`);

  progress(2, 'Discovering symbol definitions...');
  const discoverer = new DefinitionDiscoverer();
  const symbolTable = await discoverer.discover(files);
  console.error(`Discovered ${symbolTable.symbols.size} symbols`);

  progress(3, 'Tracking references...');
  const refTracker = new ReferenceTracker(symbolTable, projectRoot);
  const referenceMap = await refTracker.track(files);
  const totalRefs = Array.from(referenceMap.references.values()).reduce(
    (sum, refs) => sum + refs.length,
    0
  );
  console.error(`Found ${totalRefs} references`);

  progress(4, 'Detecting escape hatches...');
  const escapeDetector = new EscapeHatchDetector();
  const escapeHatches = await escapeDetector.detect(files);
  console.error(`Found ${escapeHatches.all.length} escape hatches`);

  progress(5, 'Detecting entry points...');
  const entryDetector = new EntryPointDetector();
  const entryPoints = entryDetector.detect(projectRoot);
  console.error(`Found ${entryPoints.length} entry points`);

  progress(6, 'Analyzing reachability...');
  const reachability = new ReachabilityAnalyzer(symbolTable, referenceMap, entryPoints);
  const reachableSymbols = reachability.analyze();
  console.error(`${reachableSymbols.size} symbols are reachable`);

  progress(7, 'Classifying symbols...');
  const classifier = new SymbolClassifier(
    symbolTable,
    referenceMap,
    escapeHatches,
    reachableSymbols,
    entryPoints
  );
  const result = classifier.classify();

  // Update metadata with duration and project root
  const duration = Date.now() - startTime;
  result.metadata.durationMs = duration;
  result.metadata.projectRoot = projectRoot;

  console.error(`\nAnalysis complete in ${duration}ms`);
  console.error(`- Category 1 (Definitely Dead): ${result.category1_definitelyDead.length}`);
  console.error(`- Category 2 (Needs Review): ${result.category2_needsReview.length}`);
  console.error(`- Potential lines saved: ~${result.statistics.potentialLOCSaved}`);

  return result;
}

/**
 * Main execution
 */
async function main(): Promise<void> {
  try {
    const { projectPath, format } = parseArgs();

    // Run analysis
    const result = await analyze(projectPath);

    // Generate report based on format
    console.error('\nGenerating report...\n');

    let report: string;
    switch (format) {
      case 'json':
        const jsonReporter = new JSONReporter();
        report = jsonReporter.generate(result);
        break;
      case 'html':
        const htmlReporter = new HTMLReporter();
        report = htmlReporter.generate(result);
        break;
      case 'markdown':
      default:
        const mdReporter = new MarkdownReporter();
        report = mdReporter.generate(result);
        break;
    }

    // Output to stdout
    console.log(report);

  } catch (error) {
    console.error('Error during analysis:');
    console.error(error);
    process.exit(1);
  }
}

// Run main function
main();
