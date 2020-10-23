import Grid from '@material-ui/core/Grid';
import React from 'react';
import { SearchInput } from '../../components/SearchInput/SearchInput';

export const Search = () => (
  <Grid container spacing={2}>
    <Grid item md={10}>
      <SearchInput multiline />
    </Grid>
  </Grid>
);
