# Dashboard for spice.ai

This is the dashboard for spice.ai that is served from `spiced` (github.com/spiceai/spiceai).

The build process copies the build assets to `/pkg/dashboard/build` where they are embedded into `spiced` in `/pkg/dashboard/embedded.go`.

## Available Scripts

During development and when started with `yarn start` the project proxies to the backend [http://localhost:8000](http://localhost:8000) which is the default server and port for the `spiced` runtime. It's expected that the spice.ai runtime is already running before starting the development server.

I.e. Dashboard frontend -> dashboard backend (localhost:3000) ---proxied--> spiced (localhost:8000).

In the project directory, you can run:

### `yarn start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### `yarn test`

Launches the test runner in the interactive watch mode.\
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

### `yarn build`

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.
