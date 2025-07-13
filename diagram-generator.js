const fs = require('fs');
const path = require('path');
const acorn = require('acorn');
const walk = require('acorn-walk');

// Recursively collect all .js files starting from a directory
function collectJsFiles(dir, files = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      collectJsFiles(full, files);
    } else if (entry.isFile() && entry.name.endsWith('.js')) {
      files.push(full);
    }
  }
  return files;
}

function parseFile(file, registry) {
  const code = fs.readFileSync(file, 'utf8');
  let ast;
  try {
    ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'module' });
  } catch (e) {
    console.warn(`Could not parse ${file}: ${e.message}`);
    return;
  }

  const fileId = file;
  registry.files[fileId] = { functions: [] };

  walk.simple(ast, {
    ImportDeclaration(node) {
      const dep = node.source.value;
      registry.edges.push({ from: fileId, to: path.resolve(path.dirname(file), dep) });
    },
    CallExpression(node) {
      if (node.callee.type === 'Identifier' && node.callee.name === 'require') {
        const arg = node.arguments[0];
        if (arg && arg.type === 'Literal') {
          const dep = arg.value;
          registry.edges.push({ from: fileId, to: path.resolve(path.dirname(file), dep) });
        }
      }
    }
  });

  // Collect functions and check for unused parameters
  walk.simple(ast, {
    FunctionDeclaration(node) {
      handleFunction(node, fileId, registry);
    },
    FunctionExpression(node) {
      handleFunction(node, fileId, registry);
    },
    ArrowFunctionExpression(node) {
      handleFunction(node, fileId, registry);
    }
  });
}

function handleFunction(node, fileId, registry) {
  // Derive a name if possible
  let name = node.id && node.id.name;
  if (!name && node.parent && node.parent.type === 'VariableDeclarator') {
    name = node.parent.id.name;
  }
  if (!name) return;

  const params = node.params.map(p => p.name).filter(Boolean);
  const used = new Set();
  walk.simple(node.body, {
    Identifier(id) {
      if (params.includes(id.name)) used.add(id.name);
    }
  });
  const unused = params.filter(p => !used.has(p));

  registry.functions[name] = { file: fileId, unused };
  registry.files[fileId].functions.push(name);

  // Collect called functions
  walk.simple(node.body, {
    CallExpression(call) {
      if (call.callee.type === 'Identifier') {
        const calledName = call.callee.name;
        registry.funcEdges.push({ from: name, to: calledName });
      }
    }
  });
}

function generateDiagram(registry, outPath) {
  const lines = ['graph LR'];

  // File to file edges
  registry.edges.forEach(({ from, to }) => {
    const f = sanitize(from);
    const t = sanitize(to);
    lines.push(`${f}["${from}"] --> ${t}["${to}"]`);
  });

  // File to functions
  for (const file in registry.files) {
    const fileId = sanitize(file);
    for (const fn of registry.files[file].functions) {
      const fnId = sanitize(fn);
      const label = registry.functions[fn].unused.length
        ? `${fn}(${registry.functions[fn].unused.join(', ')} unused)`
        : fn;
      lines.push(`${fileId} --> ${fnId}["${label}"]`);
    }
  }

  // Function call edges
  registry.funcEdges.forEach(({ from, to }) => {
    if (registry.functions[from] && registry.functions[to]) {
      const f = sanitize(from);
      const t = sanitize(to);
      lines.push(`${f} --> ${t}`);
    }
  });

  lines.push('classDef unused fill:#fdd,stroke:#f66,stroke-width:2;');

  fs.writeFileSync(outPath, lines.join('\n'), 'utf8');
}

function sanitize(str) {
  return str.replace(/[^a-zA-Z0-9_]/g, '_');
}

function main() {
  const startDir = process.argv[2] || process.cwd();
  const files = collectJsFiles(startDir);

  const registry = { files: {}, functions: {}, edges: [], funcEdges: [] };
  files.forEach(file => parseFile(file, registry));

  generateDiagram(registry, path.join(startDir, 'diagram.mmd'));
  console.log('diagram.mmd generated.');
}

if (require.main === module) {
  main();
}
