import { BrowserRouter as Router } from 'react-router-dom';
import { Container, Grid } from '@material-ui/core';
import React from 'react';

import { Navigation } from '../../components/Navigation/Navigation';
import { Search } from './Search';
import { mockSearch } from '../../services/search/search.mock';

export default {
  title: 'views/Search',
  component: Search,
};

export const Default = () => {
  mockSearch();

  return (
    <Router>
      <Container maxWidth={false} disableGutters={true}>
        <Grid container spacing={2}>
          <Grid item md={1}>
            <Navigation active="search" />
          </Grid>
          <Grid item md={11}>
            <Search />
          </Grid>
        </Grid>
      </Container>
    </Router>
  );
};
