# VTAdmin Web
VTAdmin Web is a user interface that allows Vitess users to easily manage and view state of their Vitess components. VTAdmin Web should be used with VTAdmin API (shipped within the Vitess monorepo).

## Usage
### Client-side (Rails example)
You can directly import the .js files and .css files necessary to use in a Rails-Webpacker asset pipeline, like so:
```javascript
// app/javascript/vtadmin.js
const importAll = (r) => r.keys().map(r)
// imports all media files for use
importAll(require.context('vtadmin/static/media', false, /\.(png|jpe?g|svg)$/));
// imports all css files for use
importAll(require.context('vtadmin/static/css', false, /\.(js|css)$/));
// imports all javascript files for use
importAll(require.context('vtadmin/static/js', false, /\.(js|css)$/));
```

Then in your .erb file, import the styles and js as a pack:
```
<%= javascript_pack_tag "vtadmin", "data-turbolinks-track": "reload" %>
<%= stylesheet_pack_tag "vtadmin", media: "all", "data-turbolinks-track": "reload" %>
```

### Server-side (Express.js example)
Primarily, `@planetscale/vtadmin` exports two variables: `defaultFilePath` and `directoryPath`. Server side, you can serve the entire app easily like so (express.js example):
```javascript
import express from 'express'
import * as ui from 'vtadmin'

const app = express()

app.use(express.static(ui.directoryPath))
app.get('/', function (req, res) {
    console.log("FILE PATH ", ui.defaultFilePath)
    res.sendFile(ui.defaultFilePath)
})

app.listen(9000, () => {
    console.log(`Server started on http://localhost:9000`)
})
```

## Scripts
**File structure**
```
- vtadmin
    - planetscale-vtadmin-scripts
        - index.ts // Index file that is copied into package directory as the entrypoint of npm package
        - README.md // README for @planetscale/vtadmin npm package
        - package.json // Base package.json for @planetscale/vtadmin npm package
    - scripts
        - createPlanetscaleVTAdmin.ts // Script to copy vtadmin's package.json and build folder into package folder
    - planetscale-vtadmin // Directory containing npm module @planetscale/vtadmin
```

`web/vtadmin/package.json` includes a build script run by `npm run build:planetscale-vtadmin` that:
1. Compiles contents of `web/vtadmin/planetscale-vtadmin-scripts` to a new directory `web/vtadmin/planetscale-vtadmin`

## Release NPM Package
To release new versions of the vtadmin npm package:
1. Bump the version of vtadmin at `web/vtadmin/package.json`
2. Run `npm run build` to compile vtadmin-web into a minified `build` folder
3. Run `npm run build:planetscale-vtadmin` to create package directory for `@planetscale/vtadmin` npm module
4. `cd planetscale-vtadmin` to navigate into the folder for module `@planetscale/vtadmin`
5. `npm publish --access public` to publish new version to npm

