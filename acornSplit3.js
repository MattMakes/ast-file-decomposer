const fs = require('fs');
const path = require('path');
const acorn = require('acorn');
const walk = require('acorn-walk');
const { program } = require('commander');

program
  .option('-f, --file <path>', 'input file path')
  .option('-o, --output <path>', 'output directory')
  .parse();

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
const ast = acorn.parse(code, { sourceType: 'module', ecmaVersion: 2022 });

let currentScope = {
    parent: null,
    identifiers: []
};

const scopes = [currentScope];

walk.simple(ast, {
    VariableDeclaration(node) {
        currentScope.identifiers.push(...node.declarations.map(declaration => declaration.id.name));
    },
    FunctionDeclaration(node) {
        currentScope.identifiers.push(node.id.name);
    },
    BlockStatement() {
        const newScope = { parent: currentScope, identifiers: [] };
        currentScope = newScope;
        scopes.push(newScope);
    },
    "BlockStatement:exit": function() {
        scopes.pop();
        currentScope = currentScope.parent;
    }
});

const getDependency = (name) => {
    for (let i = scopes.length - 1; i >= 0; i--) {
        if (scopes[i].identifiers.includes(name)) {
            return name;
        }
    }
    return null;
};

const getFunctionDependencies = (node) => {
    const dependencies = [];

    walk.simple(node, {
        Identifier(childNode) {
            const dependencyName = getDependency(childNode.name);
            if (dependencyName && !dependencies.includes(dependencyName)) {
                dependencies.push(dependencyName);
            }
        }
    });

    return dependencies;
};

const getImportedModules = () => {
    const modules = [];

    walk.simple(ast, {
        ImportDeclaration(node) {
            modules.push(node.source.value);
        }
    });

    return modules;
};

const functions = [];

walk.simple(ast, {
    FunctionDeclaration(node) {
        functions.push(node);
    },
    VariableDeclaration(node) {
        if (node.declarations[0].init && 
           (node.declarations[0].init.type === 'ArrowFunctionExpression' || 
            node.declarations[0].init.type === 'FunctionExpression')) {
            functions.push(node);
        }
    },
    AssignmentExpression(node) {
        if (node.right.type === 'FunctionExpression') {
            functions.push(node);
        }
    }
});

const writeFunctionToFile = (node, functionName, dependencies) => {
    const fileContent = [
        ...getImportedModules().map(module => `const ${path.basename(module, '.js')} = require('${module}');`),
        ...dependencies.map(dep => `const ${dep} = require('./${dep}');`),
        code.slice(node.start, node.end),
        `module.exports = ${functionName};`
    ].join('\n\n');

    fs.writeFileSync(path.join(outputDir, `${functionName}.js`), fileContent, 'utf-8');
};

const indexContent = [];

functions.forEach(node => {
    let functionName;
    switch (node.type) {
        case 'FunctionDeclaration':
            functionName = node.id.name;
            break;
        case 'VariableDeclaration':
            functionName = node.declarations[0].id.name;
            break;
        case 'AssignmentExpression':
            functionName = node.left.property.name;
            break;
    }

    if (functionName) {
        const dependencies = getFunctionDependencies(node);
        writeFunctionToFile(node, functionName, dependencies);
        indexContent.push(`const ${functionName} = require('./${functionName}');`);
    }
});

indexContent.push(`module.exports = { ${functions.map(fn => {
    switch (fn.type) {
        case 'FunctionDeclaration':
            return fn.id.name;
        case 'VariableDeclaration':
            return fn.declarations[0].id.name;
        case 'AssignmentExpression':
            return fn.left.property.name;
    }
}).join(', ')} };`);

fs.writeFileSync(path.join(outputDir, 'index.js'), indexContent.join('\n'), 'utf-8');

console.log('Processing completed.');
