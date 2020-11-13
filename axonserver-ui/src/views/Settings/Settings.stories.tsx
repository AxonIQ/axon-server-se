import { BrowserRouter as Router } from 'react-router-dom';
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
import '../../app.scss';

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
    <Router>
      <div className="app-root">
        <Navigation active="settings" />
        <Container maxWidth={false}>
          <Grid item md={12}>
            <Settings />
          </Grid>
        </Container>
      </div>
    </Router>
  );
};
