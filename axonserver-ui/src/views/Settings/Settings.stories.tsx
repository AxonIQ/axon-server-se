import { Container, Grid } from '@material-ui/core';
import React from 'react';
import { setupWorker } from 'msw';

import { mockGetStatus } from '../../services/public/status/status.mock';
import { Navigation } from '../../components/Navigation/Navigation';
import { mockGetMe } from '../../services/me/me.mock';
import { mockGetPublic } from '../../services/public/public.mock';
import { mockGetVersion } from '../../services/version/version.mock';
import { mockGetVisibleContexts } from '../../services/visibleContexts/visibleContexts.mock';
import { Settings } from './Settings';

export default {
  title: 'Views/Settings',
  component: Settings,
};

export const Default = () => {
  setupWorker(
    mockGetVersion,
    mockGetMe,
    mockGetPublic,
    mockGetStatus,
    mockGetVisibleContexts,
  ).start();

  return (
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
};
