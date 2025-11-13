import * as fs from 'fs';
import * as path from 'path';
import type { EntryPoint, EntryPointType } from './types.ts';

export class EntryPointDetector {
  detect(projectRoot: string): EntryPoint[] {
    const entries: EntryPoint[] = [];
    const pkgPath = path.join(projectRoot, 'package.json');
    if (fs.existsSync(pkgPath)) {
      try {
        const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf-8'));
        if (pkg.main) {
          const mainPath = path.resolve(projectRoot, pkg.main);
          entries.push({
            type: 'package-main' as EntryPointType,
            filePath: mainPath,
            context: 'package.json main',
          });
        }
      } catch (e) {}
    }
    return entries;
  }
}
