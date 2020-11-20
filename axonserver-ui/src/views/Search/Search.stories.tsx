import { BrowserRouter as Router } from 'react-router-dom';
import { Container, Grid } from '@material-ui/core';
import React from 'react';

import { Navigation } from '../../components/Navigation/Navigation';
import { Search } from './Search';
import { mockSearch } from '../../services/search/search.mock';
import '../../app.scss';

export default {
  title: 'views/Search',
  component: Search,
};

export const Default = () => {
  mockSearch();

  return (
    <Router>
      <div className="app-root">
        <Navigation active="search" />
        <Container maxWidth={false}>
          <Grid item md={12}>
            <Search />
          </Grid>
        </Container>
      </div>
    </Router>
  );
};
