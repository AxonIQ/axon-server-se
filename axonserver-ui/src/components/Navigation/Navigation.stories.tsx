import { Grid } from '@material-ui/core';
import React from 'react';
import { Navigation } from './Navigation';

export default {
  title: 'Components/Navigation',
  component: Navigation,
};

export const Default = () => (
  <Grid container>
    <Grid md={1}>
      <Navigation />
    </Grid>
  </Grid>
);
export const SearchActive = () => (
  <Grid container>
    <Grid md={1}>
      <Navigation active="search" />
    </Grid>
  </Grid>
);
