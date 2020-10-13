import React from 'react';

import { addDecorator } from '@storybook/react';
import { ThemeProvider } from '@material-ui/core/styles';

import { theme } from '../src/theme';

export const parameters = {
  actions: { argTypesRegex: '^on[A-Z].*' },
  backgrounds: {
    values: [
      { name: 'dash-background', value: '#e3e4e6' },
      { name: 'white', value: '#ffffff' },
      { name: 'dark', value: '#333' },
    ],
    default: 'dash-background',
  },
};

addDecorator((story) => <ThemeProvider theme={theme}>{story()}</ThemeProvider>);
