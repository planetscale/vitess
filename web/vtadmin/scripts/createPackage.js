"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs = __importStar(require("fs-extra"));
const path = __importStar(require("path"));
const packageJsonPath = path.join(__dirname, '..', 'package.json');
const buildPath = path.join(__dirname, '..', 'build');
const uiPackagePath = path.join(__dirname, '..', 'package');
const indexJsPath = path.join(uiPackagePath, 'index.js');
const indexDtsPath = path.join(uiPackagePath, 'index.d.ts');
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        console.log(`Verifying ${indexJsPath} exists`);
        try {
            yield fs.ensureFile(indexJsPath);
        }
        catch (ex) {
            throw new Error(`${indexJsPath} did not exist.`);
        }
        console.log(`Verifying ${indexDtsPath} exists`);
        try {
            yield fs.ensureFile(indexDtsPath);
        }
        catch (ex) {
            throw new Error(`${indexDtsPath} did not exist.`);
        }
        console.log(`Copy: ${buildPath} to ${uiPackagePath}`);
        try {
            yield fs.ensureDir(buildPath);
            yield fs.copy(buildPath, uiPackagePath);
        }
        catch (e) {
            throw e;
        }
        console.log(`Reading package.json from: ${packageJsonPath}`);
        try {
            const packageJsonObj = yield fs.readJson(packageJsonPath);
            const { name, version, description, keywords, author, repository, license, publishConfig } = packageJsonObj;
            console.log(`Found name: ${name} version: ${version}`);
            const newPackageJson = {
                name,
                version,
                description,
                keywords,
                author,
                repository,
                license,
                main: "index.js",
                typings: "index.d.ts",
                homepage: "./",
                publishConfig
            };
            const newPackageJsonFilePath = path.join(uiPackagePath, 'package.json');
            console.log(`Writing new package.json to ${newPackageJsonFilePath}`);
            yield fs.writeJson(newPackageJsonFilePath, newPackageJson, { spaces: '  ' });
        }
        catch (e) {
            throw e;
        }
    });
}
main();
process.on('unhandledRejection', e => { throw e; });
