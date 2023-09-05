const fs = require('fs');
const acorn = require('acorn');
const walk = require('acorn-walk');
const { program } = require('commander');

function extractIdentifiers(node) {
  const ids = new Set();
  walk.simple(node, {
    Identifier(childNode) {
      ids.add(childNode.name);
    }
  });
  return ids;
}

program
  .option('-f, --file <path>', 'input file path')
  .option('-o, --output <path>', 'output directory')

program.parse();

const options = program.opts();
if (!options.file) {
  console.error('You must provide an input file path using the -f option.');
  process.exit(1);
}

const filePath = options.file;
if (!fs.existsSync(filePath)) {
  console.error(`The file at path ${filePath} does not exist.`);
  process.exit(1);
}

const outputDir = options.output || './';

if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir);
}

const code = fs.readFileSync(filePath, 'utf-8');
const ast = acorn.parse(code, {
  sourceType: 'module',
  ecmaVersion: 2020
});

const functions = [];
const dependencies = [];

// walk the AST
walk.simple(ast, {
  FunctionDeclaration(node) {
    functions.push(node);
  },
  VariableDeclaration(node) {
    if (node.declarations[0] && node.declarations[0].init &&
      (node.declarations[0].init.type === 'ArrowFunctionExpression' || node.declarations[0].init.type === 'FunctionExpression')) {
      functions.push(node);
    }
  },
  ImportDeclaration(node) {
    dependencies.push(node);
  }
});

const topLevelIdentifiers = new Set();

walk.simple(ast, {
  VariableDeclarator(node) {
    if (node.id && node.id.name) {
      topLevelIdentifiers.add(node.id.name);
    }
  },
  FunctionDeclaration(node) {
    if (node.id && node.id.name) {
      topLevelIdentifiers.add(node.id.name);
    }
  }
});


// const writeFunctionToFile = (func) => {
//   let funcName = '';
//   if (func.type === 'FunctionDeclaration') {
//     funcName = func.id.name;
//   } else if (func.type === 'VariableDeclaration') {
//     funcName = func.declarations[0].id.name;
//   }

//   const content = dependencies.map(dep => code.slice(dep.start, dep.end)).join('\n') + '\n\n' + code.slice(func.start, func.end) + '\n\nmodule.exports = ' + funcName + ';';
//   fs.writeFileSync(`${outputDir}/${funcName}.js`, content, 'utf-8');
// };

const writeFunctionToFile = (func) => {
  let funcName = '';
  if (func.type === 'FunctionDeclaration') {
    funcName = func.id.name;
  } else if (func.type === 'VariableDeclaration') {
    funcName = func.declarations[0].id.name;
  }

  const identifiers = [...extractIdentifiers(func)];
  const dependenciesForFunction = dependencies.filter(dep => {
    return identifiers.includes(dep.source.value);
  });
  // const topLevelDependenciesForFunction = [...topLevelIdentifiers].filter(id => identifiers.includes(id));

  const topLevelDependenciesForFunction = [...topLevelIdentifiers].filter(id => identifiers.includes(id)).map(id => {
    const node = functions.find(func =>
      (func.type === 'FunctionDeclaration' && func.id.name === id) ||
      (func.type === 'VariableDeclaration' && func.declarations[0].id.name === id)
    );

    if (node) {
      return code.slice(node.start, node.end);
    } else {
      console.warn(`Warning: Couldn't find the source for identifier ${id}.`);
      return null; // or '' if you prefer an empty string
    }
  }).filter(Boolean);

  const content = [
    ...dependenciesForFunction.map(dep => code.slice(dep.start, dep.end)),
    ...topLevelDependenciesForFunction,
    code.slice(func.start, func.end),
    `\n\nmodule.exports = ${funcName};`
  ].join('\n');

  fs.writeFileSync(`${outputDir}/${funcName}.js`, content, 'utf-8');
};

functions.forEach(writeFunctionToFile);

// Generate the index.js file
const indexContent = functions.map(func => {
  let funcName = '';
  if (func.type === 'FunctionDeclaration') {
    funcName = func.id.name;
  } else if (func.type === 'VariableDeclaration') {
    funcName = func.declarations[0].id.name;
  }
  return `const ${funcName} = require('./${funcName}');`;
}).join('\n') + '\n\n' + 'module.exports = {' + functions.map(func => {
  let funcName = '';
  if (func.type === 'FunctionDeclaration') {
    funcName = func.id.name;
  } else if (func.type === 'VariableDeclaration') {
    funcName = func.declarations[0].id.name;
  }
  return funcName;
}).join(', ') + '};';

fs.writeFileSync(`${outputDir}/index.js`, indexContent, 'utf-8');
