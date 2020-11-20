import React from 'react';

import { addDecorator } from '@storybook/react';
import { ThemeProvider } from '@material-ui/core/styles';
import './mockevent';

import { theme } from '../src/theme';
import './preview.scss';

export const parameters = {
  actions: { argTypesRegex: '^on[A-Z].*' },
  layout: 'fullscreen',
  backgrounds: {
    values: [
      { name: 'dash-background', value: '#e3e4e6' },
      { name: 'white', value: '#ffffff' },
      { name: 'dark', value: '#333' },
    ],
    default: 'dash-background',
  },
};

export const globalTypes = {
  MockEvent: window.MockEvent,
};

addDecorator((story) => <ThemeProvider theme={theme}>{story()}</ThemeProvider>);
