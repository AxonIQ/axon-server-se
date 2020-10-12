import React from 'react';

import { addDecorator } from '@storybook/react';
import { ThemeProvider } from '@material-ui/core/styles';

import { theme } from '../src/theme';

export const parameters = {
  actions: { argTypesRegex: "^on[A-Z].*" },
  backgrounds: {
    default: 'light'
  }
}

addDecorator((story) => (
  <ThemeProvider theme={theme}>{story()}</ThemeProvider>
));