import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';
import { ThemeProvider } from '@material-ui/core/styles';
import { theme } from './theme';
import './index.scss';

ReactDOM.render(
  <React.StrictMode>
    <div className="main">
      <ThemeProvider theme={theme}>
        <App />
      </ThemeProvider>
    </div>
  </React.StrictMode>,
  document.getElementById('root'),
);

// Hot Module Replacement (HMR) - Remove this snippet to remove HMR.
// Learn more: https://www.snowpack.dev/#hot-module-replacement
if (import.meta.hot) {
  import.meta.hot.accept();
}
