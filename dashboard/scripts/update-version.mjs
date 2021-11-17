import fs from "fs";
import path from "path";

const versionFilePath = path.resolve("../version.txt");
const constantsFilePath = path.resolve("src/constants.ts");

console.log("Updating version...");

console.log(`Reading ${versionFilePath}...`);
const versionContent = fs.readFileSync(versionFilePath, "utf8");
console.log(`Read version: ${versionContent}`);

console.log(`Reading ${constantsFilePath}...`);
const originalConstantsContent = fs.readFileSync(constantsFilePath, "utf8");
const replacementVersionFile = `SPICE_VERSION = 'v${versionContent}'`;

const updatedContent = originalConstantsContent.replace(
  new RegExp("SPICE_VERSION = .*", "g"),
  replacementVersionFile
);

fs.writeFileSync(constantsFilePath, updatedContent);
console.log(`Updated constants file with line: ${replacementVersionFile}.`);
