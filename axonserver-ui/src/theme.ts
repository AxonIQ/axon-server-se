import { createMuiTheme } from '@material-ui/core';
import './theme.scss';

export const theme = createMuiTheme({
  palette: {
    primary: {
      main: '#f35c00',
    },
    secondary: {
      main: '#0da7a5',
    },
  },
  typography: {
    fontFamily: ['Lato', 'monospace'].join(','),
  },
});
