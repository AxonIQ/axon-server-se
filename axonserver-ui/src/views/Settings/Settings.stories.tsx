import { Container, Grid } from '@material-ui/core';
import React from 'react';
import { Navigation } from '../../components/Navigation/Navigation';
import { Settings } from './Settings';

export default {
  title: 'Views/Settings',
  component: Settings,
};

export const Primary = () => (
  <Container maxWidth={false} disableGutters={true}>
    <Grid container spacing={2}>
      <Grid item md={1}>
        <Navigation active="settings" />
      </Grid>
      <Grid item md={11}>
        <Settings />
      </Grid>
    </Grid>
  </Container>
);
