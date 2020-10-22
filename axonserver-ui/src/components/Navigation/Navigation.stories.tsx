import { Grid } from '@material-ui/core';
import React from 'react';
import { BrowserRouter as Router } from 'react-router-dom';
import { Navigation } from './Navigation';

export default {
  title: 'Components/Navigation',
  component: Navigation,
};

export const Default = () => (
  <Router>
    <Grid container>
      <Grid md={1}>
        <Navigation />
      </Grid>
    </Grid>
  </Router>
);
export const SearchActive = () => (
  <Router>
    <Grid container>
      <Grid md={1}>
        <Navigation active="search" />
      </Grid>
    </Grid>
  </Router>
);
