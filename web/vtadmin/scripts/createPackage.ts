import * as fs from 'fs-extra'
import * as path from 'path'

const packageJsonPath = path.join(__dirname, '..', 'package.json')
const buildPath = path.join(__dirname, '..', 'build')
const uiPackagePath = path.join(__dirname, '..', 'package')
const indexJsPath = path.join(uiPackagePath, 'index.js')
const indexDtsPath = path.join(uiPackagePath, 'index.d.ts')

async function main() {
    console.log(`Verifying ${indexJsPath} exists`)
    try {
        await fs.ensureFile(indexJsPath)
    }
    catch (ex) {
        throw new Error(`${indexJsPath} did not exist.`)
    }

    console.log(`Verifying ${indexDtsPath} exists`)
    try {
        await fs.ensureFile(indexDtsPath)
    }
    catch (ex) {
        throw new Error(`${indexDtsPath} did not exist.`)
    }

    console.log(`Copy: ${buildPath} to ${uiPackagePath}`)
    try {
        await fs.ensureDir(buildPath)
        await fs.copy(buildPath, uiPackagePath)
    }
    catch (e) {
        throw e
    }

    console.log(`Reading package.json from: ${packageJsonPath}`)
    try {
        const packageJsonObj = await fs.readJson(packageJsonPath)
        const { name, version, description, keywords, author, repository, license, publishConfig } = packageJsonObj
        console.log(`Found name: ${name} version: ${version}`)

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
        }

        const newPackageJsonFilePath = path.join(uiPackagePath, 'package.json')
        console.log(`Writing new package.json to ${newPackageJsonFilePath}`)
        await fs.writeJson(newPackageJsonFilePath, newPackageJson, { spaces: '  ' })
    }
    catch (e) {
        throw e
    }
}

main()

process.on('unhandledRejection', e => { throw e })