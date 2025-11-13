// Example TypeScript code to test the detector

export function usedFunction() {
  return "I'm used!";
}

function unusedFunction() {
  return "I'm never called";
}

export class UsedClass {
  public method() {
    console.log("Called");
  }

  private unusedPrivateMethod() {
    console.log("Never called");
  }
}

class UnusedClass {
  constructor() {
    console.log("Never instantiated");
  }
}

export const USED_CONST = 42;
const UNUSED_CONST = "dead";

// This will use the exported items
const instance = new UsedClass();
instance.method();
console.log(USED_CONST);
usedFunction();
