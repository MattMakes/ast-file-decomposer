const acorn = require('acorn');
const walk = require('acorn-walk');
const fs = require('fs');
const { program } = require('commander');

program
    .option('-f, --file <path>', 'input file path')
    .option('-o, --output <path>', 'output directory');

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
const ast = acorn.parse(code, { ecmaVersion: 2020, sourceType: 'module' });

const functions = [];
const topLevelIdentifiers = new Set();

walk.simple(ast, {
    FunctionDeclaration(node) {
        functions.push(node);
        if (node.id && node.id.name) {
            topLevelIdentifiers.add(node.id.name);
        }
    },
    VariableDeclaration(node) {
        functions.push(node);
        node.declarations.forEach(declaration => {
            if (declaration.id && declaration.id.name) {
                topLevelIdentifiers.add(declaration.id.name);
            }
        });
    },
    ImportDeclaration(node) {
        topLevelIdentifiers.add(node.source.value);
    }
});

// const writeFunctionToFile = (node, dependencies) => {
//     const functionName = node.type === 'FunctionDeclaration' ? node.id.name : node.declarations[0].id.name;

//     const requiredCodeForFunction = dependencies.map(id => {
//         const node = functions.find(func =>
//             (func.type === 'FunctionDeclaration' && func.id.name === id) ||
//             (func.type === 'VariableDeclaration' && func.declarations[0].id.name === id)
//         );

//         if (node) {
//             return code.slice(node.start, node.end);
//         } else {
//             console.warn(`Warning: Couldn't find the source for identifier ${id}.`);
//             return null;
//         }
//     }).filter(Boolean).join('\n');

//     const functionCode = code.slice(node.start, node.end);
//     const fileContent = `${requiredCodeForFunction}\n\n${functionCode}`;
//     fs.writeFileSync(`${outputDir}/${functionName}.js`, fileContent, 'utf-8');
// };

const writeFunctionToFile = (node, dependencies) => {
  const isFunctionDeclaration = node.type === 'FunctionDeclaration';
  const isFunctionExpression = node.type === 'VariableDeclaration' && (node.declarations[0].init && (node.declarations[0].init.type === 'ArrowFunctionExpression' || node.declarations[0].init.type === 'FunctionExpression'));

  if (!isFunctionDeclaration && !isFunctionExpression) {
      return; // Skip non-functions
  }

  const functionName = isFunctionDeclaration ? node.id.name : node.declarations[0].id.name;

  const requiredCodeForFunction = dependencies.map(id => {
      const dependencyNode = functions.find(func =>
          (func.type === 'FunctionDeclaration' && func.id.name === id) ||
          (func.type === 'VariableDeclaration' && func.declarations[0].id.name === id)
      );

      if (dependencyNode) {
          return code.slice(dependencyNode.start, dependencyNode.end);
      } else {
          console.warn(`Warning: Couldn't find the source for identifier ${id}.`);
          return null;
      }
  }).filter(Boolean).join('\n');

  const functionCode = code.slice(node.start, node.end);
  const fileContent = `${requiredCodeForFunction}\n\n${functionCode}\n\nmodule.exports = ${functionName};`;
  fs.writeFileSync(`${outputDir}/${functionName}.js`, fileContent, 'utf-8');
};

functions.forEach(node => {
    const identifiers = new Set();
    walk.simple(node, {
        Identifier(childNode) {
            identifiers.add(childNode.name);
        }
    });

    const topLevelDependenciesForFunction = [...topLevelIdentifiers].filter(id => identifiers.has(id));

    writeFunctionToFile(node, topLevelDependenciesForFunction);
});

// const indexContent = functions.map(node => {
//     const functionName = node.type === 'FunctionDeclaration' ? node.id.name : node.declarations[0].id.name;
//     return `const ${functionName} = require('./${functionName}');`;
// }).join('\n') + '\n\n' + 'module.exports = {' + functions.map(node => {
//     const functionName = node.type === 'FunctionDeclaration' ? node.id.name : node.declarations[0].id.name;
//     return `${functionName},`;
// }).join('\n') + '};';

const indexContent = functions.map(node => {
  const isFunctionDeclaration = node.type === 'FunctionDeclaration';
  const isFunctionExpression = node.type === 'VariableDeclaration' && (node.declarations[0].init && (node.declarations[0].init.type === 'ArrowFunctionExpression' || node.declarations[0].init.type === 'FunctionExpression'));

  if (!isFunctionDeclaration && !isFunctionExpression) {
      return null; // Skip non-functions
  }

  const functionName = isFunctionDeclaration ? node.id.name : node.declarations[0].id.name;
  return `const ${functionName} = require('./${functionName}');`;
}).filter(Boolean).join('\n') + '\n\n' + 'module.exports = {' + functions.map(node => {
  const isFunctionDeclaration = node.type === 'FunctionDeclaration';
  const isFunctionExpression = node.type === 'VariableDeclaration' && (node.declarations[0].init && (node.declarations[0].init.type === 'ArrowFunctionExpression' || node.declarations[0].init.type === 'FunctionExpression'));

  if (!isFunctionDeclaration && !isFunctionExpression) {
      return null; // Skip non-functions
  }

  const functionName = isFunctionDeclaration ? node.id.name : node.declarations[0].id.name;
  return `${functionName},`;
}).filter(Boolean).join('\n') + '};';

fs.writeFileSync(`${outputDir}/index.js`, indexContent, 'utf-8');
