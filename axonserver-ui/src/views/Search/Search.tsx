import Grid from '@material-ui/core/Grid';
import React from 'react';
import { SearchQueryField } from '../../components/SearchQueryField/SearchQueryField';

export const Search = () => (
  <Grid container spacing={2}>
    <Grid item md={12}>
      <SearchQueryField multiline />
    </Grid>
  </Grid>
);
